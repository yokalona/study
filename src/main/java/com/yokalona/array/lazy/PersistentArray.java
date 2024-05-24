package com.yokalona.array.lazy;

import com.yokalona.array.lazy.configuration.Configuration;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;
import com.yokalona.array.lazy.subscriber.Subscriber;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


/**
 * <p>Represents a persistent array. Such an array is stored on disk or any other storage and depending on implementation
 * can provide different levels of consistency. Basically, every read/write operation can cause an object to execute one
 * or multiple read or write operations. The best effort is taken. Since persistent layer requires manipulation with
 * resources(disk or network), it is important to properly <bold>close</bold> such an array. If @see #close method was
 * not called properly after work was done, no guaranties about data consistency are given.</p>
 * <p>Persistent array support multiple modes of persistent layer structure. There are four modification over file
 * format, each affecting different properties, such as search speed, faster read/write operations, data storage and
 * network savings, However, there are gains and losses, one should be careful in choosing configuration as it might and
 * will drastically affect performance.</p>
 * <p>At the moment, Persistent array considered to be <bold>not thread safe</bold>, some level of thread safety can by
 * acquired by manipulating access mode of underlying file, using file system as a final orbiter in resolving concurrent
 * modifications. While it might be tempting, one should avoid it until it absolutely necessary, as rws and rwd modes are
 * painfully slow.</p>
 * <p>Any implementation of persistent array should check version of a persistent storage, because with updated version
 * file format could change, the only exception is minor version.</p>
 * <p>Serialisation is used based on a type descriptors, that describes the type being stored in an array. No multiple
 * types arrays, not multiple type descriptors are supported.</p>
 * <p>
 * Default implementation
 *
 * @param <Type> type of underlying array
 */
public class PersistentArray<Type> implements AutoCloseable {

    private static final byte[] HEADER = new byte[]{-0x22, -0x36, -0x26, -0x06, -0x36, -0x26};

    /**
     * Version mark, represented as 4-byte word.
     * <pre>
     *     <ul>
     *         <li>0-byte - critical version, no backward and no forward compatibility</li>
     *         <li>1-byte - major version, forward compatible changes only</li>
     *         <li>2-byte - minor version, backward and forward compatible changes</li>
     *         <li>3-byte - storing algorithm: word = <b>(AA BB CC DD)2<b/>:</li>
     *         <ul>
     *             <li>DD = 00, non-fixed array, storing keys and offsets in different files, reduces storage
     *             consumption and decreases fragmentation, however requires longer seek operations, update operations
     *             might take longer. Best suitable for NVM based storages</li>
     *             <li>DD = 01, fixed array, where each object is padded to fix exact size no matter how much actual
     *             space it takes, for example, in general, integer takes 4 bytes to be stored, however for string the
     *             size depends on its content and is not known ahead of time, in such a case, the size of a string
     *             should be predefined, for example, let's say that any string stored in this exact array has 256 bytes
     *             of data. No matter on how much actually space is taken, the persistent layer will pad data to be
     *             exact 256 bytes and will trim any excess data. For each data point, this way of storing data actually
     *             requires one additional byte for each object to get around nullability of data.</li>
     *             <li>DD = 10, reserved</li>
     *             <li>DD = 11, reserved</li>
     *         </ul>
     *         <ul>
     *             <li>CC = 00, non-chunked, array is stored as is</li>
     *             <li>CC = 01, an array is divided by chunks. Chunk is loaded and stored as a whole in an atomic
     *             operation. The size of a chunk will be stored in the next two bytes</li>
     *             <li>CC = 10, an array is divided by compressed chunks. Chunk is loaded and stored as a whole in an
     *             atomic operation. On store operation chunk is archived using an available algorithm, for example, LZ4
     *             or similar. Using the same algorithm, it will be unarchived on any read operation.</li>
     *             <li>CC = 11, hot-chunk mode. If very same keys are regularly updated and read hot mode will put them
     *             in separate fixed size file to allow quick read/write operations. However, this file will have fixed
     *             small size and eviction process will be costly</li>
     *         </ul>
     *         <ul>
     *             <li>BB = 00, keys are stored sequentially, optimal for fast sequential read and write operations,
     *             however search will be slower, depending on other settings it might not have any guaranty about
     *             performance impact</li>
     *             <li>BB = 01, keys are stored optimal for quick search operation, however any sequential operation
     *             will be slower and highly dependant on other configuration setting and/or file system and OS.
     *             Depending on use case, it might be reasonable to first use BB = 00, then convert array to BB = 01, if
     *             it is expected that little to no sequential operation will happen in near future.</li>
     *             <li>BB = 10, reserved</li>
     *             <li>BB = 11, reserved</li>
     *         </ul>
     *         <ul>
     *             <li>AA = 00, reserved</li>
     *             <li>AA = 01, reserved</li>
     *             <li>AA = 10, reserved</li>
     *             <li>AA = 11, reserved</li>
     *         </ul>
     *     </ul>
     * </pre>
     */
    private static final byte[] VERSION = new byte[]{0x01, 0x01, 0x00, 0x00};
    public static final int HEADER_SIZE = HEADER.length + VERSION.length + (2 * SerializerStorage.INTEGER.descriptor().size());

    private final int length;
    private final Object[] data;
    private final ChunkQueue queue;
//    private final LRUCache lruCache;

    private final CachedFile storage;
    private final byte[] reusableBuffer;
    private final DataLayout dataLayout;
    private final TypeDescriptor<Type> type;
    private final FixedBiDirectionalMap read;
    public final Configuration configuration;

    private PersistentArray(int length, TypeDescriptor<Type> type, Object[] data, LayoutProvider layoutProvider,
                            Configuration configuration) {
        this.type = type;
        this.data = data;
        this.length = length;
        this.configuration = configuration;
        this.dataLayout = layoutProvider.provide(type);
        this.read = new FixedBiDirectionalMap(data.length);
        this.storage = new CachedFile(configuration.file());
        this.queue = new ChunkQueue(configuration.write().size());
//        this.lruCache = new LRUCache(configuration.memory().count());
        this.reusableBuffer = new byte[configuration.file().buffer()];
    }

    public PersistentArray(int length, TypeDescriptor<Type> type, LayoutProvider layoutProvider, Configuration configuration) {
        this(length, type, new Object[Math.min(length, configuration.memory().size())], layoutProvider, configuration);
        serialise();
    }

    /**
     * Returns item from the array. Loads from disk if necessary. Depending on configuration, may unload the least
     * recently used item in an array that is loaded into memory.
     *
     * @param index of item to return
     * @return always loaded item in the array. It is guaranteed that the returned item is at the time of return is
     * fully loaded into memory. To override this behavior, one might use proxy classes.
     */
    @SuppressWarnings("unchecked")
    public final Type
    get(int index) {
        assert index >= 0 && index < length;

        if (!read.containsKey(index)) load(index);

        return (Type) data[read.getKey(index)];
    }

    public final void
    set(int index, Type value) {
        assert index >= 0 && index < length;

        associate(index, value);
        if (configuration.write().chunked()) {
            if (queue.add(index)) flush();
        } else serialise(index);

//        assert lruCache.nodes.size() <= configuration.memory().count();
    }

    public int
    length() {
        return data.length;
    }

    public int
    loaded() {
//        return lruCache.nodes.size();
        return 0;
    }

    public void
    clear() throws IOException {
        close();
        Files.deleteIfExists(configuration.file().path());
        Arrays.fill(data, null);
//        lruCache.clear();
        queue.clear();
    }

    private void
    unload() {
//        for (int index : lruCache.nodes.keySet()) unload(index);
    }

    private void
    unload(int index) {
        if (index < 0) return;
        assert index < length;

        data[index % configuration.memory().size()] = null;
    }

    private void
    serialise() {
        try (storage; OutputWriter writer = new OutputWriter(storage.get(), reusableBuffer)) {
            writer.write(HEADER);
            byte[] version = Arrays.copyOf(VERSION, 4);
            version[3] = 0b0_00_00_01;
            writer.write(version);
            Serializer<Integer> integer = SerializerStorage.INTEGER;
            writer.write(integer.serialize(length));
            Serializer<Type> serializer = SerializerStorage.get(type);
            writer.write(integer.serialize(serializer.descriptor().size()));
            for (int index = 0; index < length; index ++) {
                writer.write(serializer.serialize(null));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void
    load(int index) {
        deserialize(index, configuration.read().chunked() ? configuration.read().size() : 1);
    }

    private void associate(int index, Type value) {
        data[index % configuration.memory().size()] = value;
        read.put(index, index % configuration.memory().size());
    }

    private void
    serialise(int index) {
        assert index >= 0 && index < length;

        try (storage; OutputWriter writer = new OutputWriter(storage.get(), reusableBuffer)) {
            dataLayout.seek(index, storage.peek());
            serialize(writer, index);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void
    serialiseChunk() {
        try (storage; OutputWriter writer = new OutputWriter(storage.get(), reusableBuffer)) {
            if (queue.count == 0) return;
            int prior = queue.first, current;
            dataLayout.seek(prior, storage.peek());
            serialize(writer, prior);
            while ((current = queue.set.nextSetBit(prior + 1)) != -1) {
                if (current != prior + 1) {
                    writer.flush();
                    dataLayout.seek(current, storage.peek());
                }
                serialize(writer, prior = current);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void
    serialize(OutputWriter writer, int index) throws IOException {
        if (!read.containsKey(index)) return;
//        get(index);
            writer.write(SerializerStorage.get(type).serialize((Type) data[read.getKey(index)]));
            for (Subscriber subscriber : configuration.subscribers()) subscriber.onSerialized(index);
//        }
    }

    private void
    deserialize(int index, int size) {
        assert index >= 0 && index < length && size >= 0;

        try (storage) {
            RandomAccessFile raf = storage.get();
            InputStream fis = new BufferedInputStream(new FileInputStream(raf.getFD()), configuration.file().buffer());
            dataLayout.seek(index, raf);
            boolean shouldSeek = false;
            byte[] datum = new byte[SerializerStorage.get(type).descriptor().size()];
            for (int offset = index; offset < Math.min(index + size, length); offset++) {
                if (read.containsKey(offset)) {
                    shouldSeek = true;
                    continue;
                } else if (shouldSeek) dataLayout.seek(offset, raf);
                int ignore = fis.read(datum);
                assert ignore == datum.length;
                associate(offset, SerializerStorage.get(type).deserialize(datum));
                for (Subscriber subscriber : configuration.subscribers()) subscriber.onDeserialized(offset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void
    close() {
        flush();
        storage.closeFile();
        unload();
    }

    public void
    flush() {
        if (configuration.write().chunked()) {
            serialiseChunk();
            queue.clear();
        }
    }

    public static <Type> void
    copy(PersistentArray<Type> from, int position, PersistentArray<Type> to, int destination, int length) {
        for (int index = 0; index < length; index++)
            to.set(destination++, from.get(position++));
    }

    public static <Type> PersistentArray<Type>
    deserialize(TypeDescriptor<Type> type, Configuration configuration) {
        return deserialize(type, configuration, new TreeSet<>());
    }

    public static <Type> PersistentArray<Type>
    deserialize(TypeDescriptor<Type> type, Configuration configuration, TreeSet<Integer> preload) {
        assert type != null && configuration != null && preload != null;

        try (RandomAccessFile raf = new RandomAccessFile(configuration.file().path().toFile(), "r");
             InputStream input = new BufferedInputStream(new FileInputStream(raf.getFD()))) {
            Serializer<Integer> integer = SerializerStorage.INTEGER;
            byte[] header = new byte[HEADER_SIZE];
            int ignore = input.read(header);
            assert ignore == header.length;
            int length = integer.deserialize(header, HEADER_SIZE - 2 * integer.descriptor().size());
            PersistentArray<Type> array = new PersistentArray<>(length, type, new Object[configuration.memory().size()], LayoutProvider.which(validHeader(header), raf), configuration);
            int boundary = configuration.memory().size();
            Iterator<Integer> iterator = preload.iterator();
            for (int index = 0; index < Math.min(boundary, preload.size()); index++) array.get(iterator.next());
            return array;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public interface LayoutProvider {
        DataLayout provide(TypeDescriptor<?> descriptor);

        static LayoutProvider
        which(byte format, RandomAccessFile raf) {
            return switch (format & 0b0000011) {
                case 0 -> ignore -> new VariableObjectLayout(raf);
                case 1 -> FixedObjectLayout::new;
                default -> throw new UnsupportedOperationException();
            };
        }
    }

    public interface DataLayout {
        void seek(int index, RandomAccessFile raf) throws IOException;
    }

    public static final class VariableObjectLayout implements DataLayout {
        private boolean cached;
        private Path offset;

        public VariableObjectLayout(RandomAccessFile ignore) {

        }

        @Override
        public void seek(int index, RandomAccessFile raf) throws IOException {
            raf.seek(findPosition(index) + HEADER_SIZE);
        }

        private long
        findPosition(int index) {
            return -1L;
        }
    }

    /**
     * Describes a fixed object data layout. Each object in such a layout has a fixed size it can occupy in the output
     * file. This layout can be beneficial for fixed size data types, such as integers or composite data types
     * consistent with fixed size data types. For large data sets, this data layout saves space on disk, however, for
     * small arrays it will create unnecessary overhead.
     *
     * @param descriptor
     */
    public record FixedObjectLayout(TypeDescriptor<?> descriptor) implements DataLayout {
        @Override
        public void seek(int index, RandomAccessFile raf) throws IOException {
            raf.seek((long) index * descriptor.size() + PersistentArray.HEADER_SIZE);
        }
    }

    private static int
    compareVersions(byte[] ignore, int offset) {
        if (VERSION[0] != ignore[offset]) return -1;
        else if (VERSION[1] < ignore[offset + 1]) return -1;

        return VERSION[2];
    }

    private static byte
    validHeader(byte[] header) {
        if (compareVersions(header, HEADER.length) < 0) throw new RuntimeException("Incompatible versions: %s =!= %s"
                .formatted(printVersion(VERSION, 0), printVersion(header, HEADER.length)));
        return header[header.length - 1];
    }

    private static String
    printVersion(byte[] version, int offset) {
        return String.format("%d.%d.%d", version[offset], version[offset + 1], version[offset + 2]);
    }

    private static class ChunkQueue {
        private final BitSet set;
        private final int capacity;

        private int count = 0;
        private int first = Integer.MAX_VALUE;

        public ChunkQueue(int capacity) {
            this.capacity = capacity;
            this.set = new BitSet(capacity);
        }

        boolean
        add(int index) {
            if (!set.get(index)) {
                first = Math.min(first, index);
                set.set(index);
                count++;
            }
            return count == capacity;
        }

        void
        clear() {
            this.set.clear();
            this.count = 0;
            this.first = Integer.MAX_VALUE;
        }

    }

    private static class LRUCache {
        private final Node head;
        private final Node tail;
        private final int capacity;
        private final Map<Integer, Node> nodes = new HashMap<>();

        public LRUCache(int capacity) {
            this.head = new Node(-1);
            this.tail = new Node(-1);
            this.head.next = this.tail;
            this.tail.prev = this.head;
            this.capacity = capacity;
        }

        public void
        raise(int key) {
            if (capacity < 0) return;
            Node node = nodes.get(key);
            remove(node);
            add(node);
        }

        public boolean
        cached(int key) {
            return nodes.containsKey(key);
        }

        public int
        cache(int key) {
            Node node = new Node(key);
            nodes.put(key, node);
            if (capacity < 0) return -1;
            add(node);

            if (nodes.size() > capacity) return pushout();
            else return -1;
        }

        public void
        clear() {
            this.head.next = this.tail;
            this.tail.prev = this.head;
            nodes.clear();
        }

        public void
        clear(int index) {
            Node node = nodes.get(index);
            remove(node);
            nodes.remove(index);
        }

        private int pushout() {
            Node toRemove = head.next;
            remove(toRemove);
            nodes.remove(toRemove.key);
            return toRemove.key;
        }

        private void
        add(Node node) {
            Node prev = tail.prev;
            prev.next = node;
            node.next = tail;
            node.prev = prev;
            tail.prev = node;
        }

        private void
        remove(Node node) {
            node.prev.next = node.next;
            node.next.prev = node.prev;
            node.next = null;
            node.prev = null;
        }

        private static class Node {
            private final int key;
            private Node next;
            private Node prev;

            public Node(int key) {
                this.key = key;
            }
        }
    }

    private static class FixedBiDirectionalMap {

        private final int capacity;
        private final Map<Integer, Integer> keys;
        private final Map<Integer, Integer> values;

        public FixedBiDirectionalMap(int capacity) {
            this.capacity = capacity;
            this.keys = new HashMap<>(capacity * 2);
            this.values = new HashMap<>(capacity * 2);
        }

        public void
        put(int key, int value) {
            if (containsValue(value)) {
                Integer prior = values.get(value);
                keys.remove(prior);
            }
            keys.put(key, value);
            values.put(value, key);

            assert keys.size() <= capacity;
            assert values.size() <= capacity;
        }

        public boolean
        containsKey(Integer key) {
            return keys.containsKey(key);
        }

        public boolean
        containsValue(Integer value) {
            return values.containsKey(value);
        }

        public int
        getKey(int key) {
            return keys.get(key);
        }

        public int
        getValue(int value) {
            return values.get(value);
        }

        public void
        clear() {
            keys.clear();
            values.clear();
        }
    }

    private record Array(int chunk, Object[] data) {

        public Array(int length, int chunk) {
            this(chunk, new Object[length]);
        }

        public void
        set(int index, Object value) {
            this.data[index % chunk] = value;
        }

        public Object
        get(int index) {
            return this.data[index % chunk];
        }
    }

}
