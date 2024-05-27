package com.yokalona.array.persitent;

import com.yokalona.array.persitent.configuration.Configuration;
import com.yokalona.array.persitent.exceptions.*;
import com.yokalona.array.persitent.io.*;
import com.yokalona.array.persitent.serializers.SerializerStorage;
import com.yokalona.array.persitent.serializers.Serializers;
import com.yokalona.array.persitent.serializers.TypeDescriptor;
import com.yokalona.array.persitent.subscriber.ChunkType;
import com.yokalona.array.persitent.subscriber.Subscriber;
import com.yokalona.array.persitent.util.Version;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;


/**
 * <p>Represents an array that stores data in a persistent manner, meaning that it is internally uses reliable storage
 * or any other means to provide at least some level of durability. Depending on configuration, implementation and any
 * other factor the data storage can be a disk, network or any other possible storage or combination of those,
 * best effort is taken.</p>
 * <p>Depending on configuration and other factors, such as collision, cache misses and any other, read/write operations
 * can cause one or multiple I/O operations, serializations and de-serializations. In other words, write operations to
 * the array might be asynchronous depending on configuration, read operations can read more than one record at the
 * time.</p>
 */
public class PersistentArray<Type> implements AutoCloseable {

    private static final boolean DELETED = true;
    private static final byte[] HEADER = new byte[]{-0x22, -0x36, -0x26, -0x06, -0x36, -0x26};
    public static final Version VERSION = new Version((byte) 1, (byte) 1, (byte) 0, (byte) 0);
    public static final int HEADER_SIZE = HEADER.length + Version.descriptor.size()
            + (2 * SerializerStorage.INTEGER.descriptor().size())
            + SerializerStorage.BOOLEAN.descriptor().size();

    /**
     * Version mark, represented as 4-byte word.
     * <pre>
     *     <ul>
     *         <li>0-byte - critical version, no backward and no forward compatibility</li>
     *         <li>1-byte - major version, forward compatible changes only</li>
     *         <li>2-byte - minor version, backward and forward compatible changes</li>
     *         <li>3-byte - storing mode: word = <b>(AA BB CC DD)2<b/>:</li>
     *         <ul>
     *             Specifies data storage layout.
     *             <li>DD = 00, reserved</li>
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
     *             Specifies paging.
     *             <li>CC = 00, reserved</li>
     *             <li>CC = 01, reserved</li>
     *             <li>CC = 10, reserved</li>
     *             <li>CC = 11, reserved</li>
     *         </ul>
     *         <ul>
     *             Specifies data mapping, linear, wavefront, etc.
     *             <li>BB = 00, reserved</li>
     *             <li>BB = 01, reserved</li>
     *             <li>BB = 10, reserved</li>
     *             <li>BB = 11, reserved</li>
     *         </ul>
     *         <ul>
     *             Specifies security and compression.
     *             <li>AA = 00, reserved</li>
     *             <li>AA = 01, reserved</li>
     *             <li>AA = 10, reserved</li>
     *             <li>AA = 11, reserved</li>
     *         </ul>
     *     </ul>
     * </pre>
     */
    private final Version version = new Version((byte) 1, (byte) 1, (byte) 0, (byte) 0);

    private final int length;
    private final ChunkQueue queue;
    private final CachedFile storage;
    private final byte[] reusableBuffer;
    private final DataLayout dataLayout;
    private final TypeDescriptor<Type> type;
    private final Configuration configuration;

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
        this.version.mode(this.dataLayout.mode());
        this.indices = new int[data.length];
        Arrays.fill(this.indices, -1);
        this.storage = new CachedFile(configuration.file());
        this.queue = new ChunkQueue(configuration.write().size());
        this.reusableBuffer = new byte[configuration.file().buffer()];
        this.readChunkSize = configuration.read().size();
    }

    /**
     * Creates a new persistent array. This operation, depending on the settings, might execute I/O operations.
     *
     * @param length         of an array in records
     * @param type           of each record
     * @param layoutProvider determines the way records are organised in storage
     * @param configuration  of an array and other components
     */
    public PersistentArray(int length, TypeDescriptor<Type> type, LayoutProvider layoutProvider, Configuration configuration) {
        this(length, type, new Object[Math.min(length, configuration.memory().size())], layoutProvider, configuration);
        serialise();
    }

    /**
     * Returns item from the persistent array. This operation might cause a data load from external resource, like disk.
     * Other records might be loaded as well depending on the current configuration. The returned record is guarantied
     * to be the most up-to-date version of the record at the moment of dispatching the method.
     */
    @SuppressWarnings("unchecked")
    public final Type
    get(int index) {
        assert index >= 0 && index < length : index + " " + length;

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
        for (int index = 0; index < length; index++) set(index, value);
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

    public Configuration
    configuration() {
        return configuration;
    }

    public void
    clear() throws IOException {
        close();
        Files.deleteIfExists(configuration.file().path());
        Arrays.fill(data, null);
        queue.clear();
    }

    public void
    serialise() {
        try (storage; OutputWriter writer = new OutputWriter(storage.get(), reusableBuffer)) {
            writer.write(HEADER);
            writer.write(Serializers.serialize(Version.descriptor, version));
            writer.write(Serializers.serialize(!DELETED));
            writer.write(Serializers.serialize(length));
            writer.write(Serializers.serialize(type.size()));
            for (int index = 0; index < length; index++) writer.write(Serializers.serialize(type, null));
            notify(Subscriber::onFileCreated);
        } catch (Exception e) {
            throw new SerializationException("during full array serialization", e);
        }
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
            throw new SerializationException("during " + index + " serialization", e);
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
            throw new SerializationException("during chunk serialization", e);
        }
    }

    private void
    serialize(OutputWriter writer, int index) throws IOException {
        if (!contains(index)) return;
        writer.write(Serializers.serialize(type, data[index % data.length]));
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
            byte[] datum = new byte[type.size()];
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
                associate(offset, Serializers.deserialize(type, datum));
                for (Subscriber subscriber : configuration.subscribers()) subscriber.onDeserialized(offset);
            }
            notify(Subscriber::onChunkDeserialized);
        } catch (IOException e) {
            throw new DeserializationException("during " + index + " deserialization", e);
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
            validateHeader(input);
            byte mode = validateVersion(input);
            validateRemovalFlag(input);
            int length = readAsType(SerializerStorage.INTEGER.descriptor(), input);
            PersistentArray<Type> array = new PersistentArray<>(length, type, new Object[configuration.memory().size()],
                    LayoutProvider.which(mode, input), configuration);
            int boundary = configuration.memory().size();
            Iterator<Integer> iterator = preload.iterator();
            for (int index = 0; index < Math.min(boundary, preload.size()); index++) array.get(iterator.next());
            return array;
        } catch (IOException e) {
            throw new DeserializationException("during full array deserialization", e);
        }
    }

    private static void
    validateHeader(InputStream input) throws IOException {
        byte[] header = read(HEADER.length, input);
        if (!Arrays.equals(HEADER, header)) throw new HeaderMismatchException();
    }

    private static byte
    validateVersion(InputStream input) throws IOException {
        Version version = readAsType(Version.descriptor, input);
        if (VERSION.compareTo(version) < 0) throw new IncompatibleVersionException(version);
        return version.mode();
    }

    private static void
    validateRemovalFlag(InputStream input) throws IOException {
        if (readAsType(SerializerStorage.BOOLEAN.descriptor(), input)) throw new FileMarkedForDeletingException();
    }

    private static <Type> Type
    readAsType(TypeDescriptor<Type> type, InputStream input) throws IOException {
        byte[] buffer = read(type.size(), input);
        return Serializers.deserialize(type, buffer);
    }

    private static byte[]
    read(int typeSize, InputStream input) throws IOException {
        byte[] buffer = new byte[typeSize];
        int ignore = input.read(buffer);
        assert ignore == buffer.length;
        return buffer;
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
            this.count--;

            assert this.count >= 0;
        }
    }

}
