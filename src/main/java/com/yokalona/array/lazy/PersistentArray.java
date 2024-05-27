package com.yokalona.array.lazy;

import com.yokalona.array.lazy.configuration.Configuration;
import com.yokalona.array.lazy.configuration.ReadChunkLimitExceededException;
import com.yokalona.array.lazy.configuration.WriteChunkLimitExceededException;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;
import com.yokalona.array.lazy.subscriber.ChunkType;
import com.yokalona.array.lazy.subscriber.Subscriber;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;


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
    private final ChunkQueue queue;
    private final CachedFile storage;
    private final byte[] reusableBuffer;
    private final DataLayout dataLayout;
    private final TypeDescriptor<Type> type;

    public final Configuration configuration;

    private Object[] data;
    private int[] indices;
    private int readChunkSize;

    private PersistentArray(int length, TypeDescriptor<Type> type, Object[] data, LayoutProvider layoutProvider,
                            Configuration configuration) {
        this.type = type;
        this.data = data;
        this.length = length;
        this.configuration = configuration;
        this.dataLayout = layoutProvider.provide(type);
        this.indices = new int[data.length];
        Arrays.fill(this.indices, -1);
        this.storage = new CachedFile(configuration.file());
        this.queue = new ChunkQueue(configuration.write().size());
        this.reusableBuffer = new byte[configuration.file().buffer()];
        this.readChunkSize = configuration.read().size();
    }

    public PersistentArray(int length, TypeDescriptor<Type> type, LayoutProvider layoutProvider, Configuration configuration) {
        this(length, type, new Object[Math.min(length, configuration.memory().size())], layoutProvider, configuration);
        serialise();
    }

    /**
     * Returns item from the persistent array. This operation might cause a data load from external resource, like disk.
     * Other records might be loaded as well depending on the current configuration. The Returned record is guarantied
     * to be the most update version of the record at the moment of dispatching the method.
     */
    @SuppressWarnings("unchecked")
    public final Type
    get(int index) {
        assert index >= 0 && index < length;

        if (configuration.read().forceReload()) load(index);
        else if (!contains(index)) {
            notify(subscriber -> subscriber.onCacheMiss(index));
            load(index);
        }

        return (Type) data[index % data.length];
    }

    /**
     * Sets the value in a persistent array. This operation might cause data to be flushed to the external resource, like
     * disk. Other records might be flushed as well depending on configuration.
     */
    public final void
    set(int index, Type value) {
        assert index >= 0 && index < length;

        int prior = indices[index % indices.length];
        if (prior >= 0 && queue.contains(prior)) {
            if (configuration.write().forceFlush()) flush();
            else {
                serialise(prior);
                queue.remove(prior);
            }
            notify(subscriber -> subscriber.onWriteCollision(prior, index));
        }

        associate(index, value);
        if (configuration.write().chunked()) {
            if (queue.add(index)) flush();
        } else serialise(index);
    }

    public final void
    fill(Type value) {
        int prior = queue.capacity;
        resizeWriteChunk(configuration.write().size());
        for (int index = 0; index < length; index ++) set(index, value);
        resizeWriteChunk(prior);
    }

    public final void
    resizeReadChunk(int newSize) {
        checkInvariant(newSize, queue.capacity, data.length);

        int prior = this.readChunkSize;
        this.readChunkSize = newSize;
        notify(subscriber -> subscriber.onChunkResized(ChunkType.READ, prior, newSize));
    }

    public final void
    resizeWriteChunk(int newSize) {
        checkInvariant(readChunkSize, newSize, data.length);

        flush();
        int prior = queue.capacity;
        queue.capacity = newSize;
        notify(subscriber -> subscriber.onChunkResized(ChunkType.WRITE, prior, newSize));
    }

    public final void
    resizeMemoryChunk(int newSize) {
        checkInvariant(readChunkSize, queue.capacity, newSize);

        flush();
        int prior = this.data.length;
        this.data = new Object[newSize];
        this.indices = new int[newSize];
        Arrays.fill(indices, -1);
        notify(subscriber -> subscriber.onChunkResized(ChunkType.MEMORY, prior, newSize));
    }

    public int
    length() {
        return length;
    }

    public void
    clear() throws IOException {
        close();
        Files.deleteIfExists(configuration.file().path());
        Arrays.fill(data, null);
        queue.clear();
    }

    private void
    checkInvariant(int read, int write, int memory) {
        if (memory < read) throw new ReadChunkLimitExceededException();
        if (memory < write) throw new WriteChunkLimitExceededException();
    }

    private boolean
    contains(int index) {
        return indices[index % indices.length] == index;
    }

    private boolean
    reload(int index) {
        return configuration.read().forceReload() || !contains(index);
    }

    private void
    notify(Consumer<Subscriber> notification) {
        configuration.subscribers().forEach(notification);
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
            for (int index = 0; index < length; index++) writer.write(serializer.serialize(null));
            notify(Subscriber::onFileCreated);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void
    load(int index) {
        deserialize(index, readChunkSize);
    }

    private void
    associate(int index, Type value) {
        indices[index % indices.length] = index;
        data[index % data.length] = value;
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
            notify(Subscriber::onChunkSerialized);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void
    serialize(OutputWriter writer, int index) throws IOException {
        if (!contains(index)) return;
        writer.write(SerializerStorage.get(type).serialize((Type) data[index % data.length]));
        notify(subscriber -> subscriber.onSerialized(index));
    }

    private void
    deserialize(int index, int size) {
        assert index >= 0 && index < length && size >= 0;

        try (storage) {
            RandomAccessFile raf = storage.get();
            InputReader reader = new InputReader(reusableBuffer, raf);
            dataLayout.seek(index, raf);
            boolean shouldSeek = false;
            byte[] datum = new byte[SerializerStorage.get(type).descriptor().size()];
            for (int offset = index; offset < Math.min(index + size, length); offset++) {
                if (!reload(offset)) {
                    shouldSeek = true;
                    if (configuration.read().breakOnLoaded()) break;
                    else continue;
                } else if (shouldSeek) {
                    reader.invalidate();
                    dataLayout.seek(offset, raf);
                }
                shouldSeek = false;
                reader.read(datum);
                associate(offset, SerializerStorage.get(type).deserialize(datum));
                for (Subscriber subscriber : configuration.subscribers()) subscriber.onDeserialized(offset);
            }
            notify(Subscriber::onChunkDeserialized);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void
    close() {
        flush();
        storage.closeFile();
    }

    public void
    flush() {
        if (configuration.write().chunked()) {
            serialiseChunk();
            queue.clear();
        }
    }

    public static <Type> void
    arraycopy(PersistentArray<Type> from, int position, PersistentArray<Type> to, int destination, int length) {
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

        try (InputStream input = new BufferedInputStream(new FileInputStream(configuration.file().path().toFile()))) {
            Serializer<Integer> integer = SerializerStorage.INTEGER;
            byte[] header = new byte[HEADER_SIZE];
            int ignore = input.read(header);
            assert ignore == header.length;
            int length = integer.deserialize(header, HEADER_SIZE - 2 * integer.descriptor().size());
            PersistentArray<Type> array = new PersistentArray<>(length, type, new Object[configuration.memory().size()],
                    LayoutProvider.which(validHeader(header), input), configuration);
            int boundary = configuration.memory().size();
            Iterator<Integer> iterator = preload.iterator();
            for (int index = 0; index < Math.min(boundary, preload.size()); index++) array.get(iterator.next());
            return array;
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    private static final class ChunkQueue {
        private final BitSet set;

        private int capacity;
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
            return count >= capacity;
        }

        boolean
        contains(int index) {
            return set.get(index);
        }

        void
        clear() {
            this.set.clear();
            this.count = 0;
            this.first = Integer.MAX_VALUE;
        }

        public void
        remove(int index) {
            if (index == first) {
                int next = set.nextSetBit(first + 1);
                if (next < 0) first = Integer.MAX_VALUE;
                else first = next;
            }

            this.set.set(index, false);
            this.count --;

            assert this.count >= 0;
        }
    }

}
