package com.yokalona.array.lazy;

import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.BitSet;
import java.util.TreeSet;

public class FixedLazyArray<Type extends FixedSizeObject> implements AutoCloseable {

    private static final byte[] HEADER = new byte[]{-34, -54, -38, -6, -54, -38};

    /**
     * Version mark, represented as 4-byte word.
     * <pre>
     *     <ul>
     *         <li>0-byte - critical version, no backward and no forward compatibility</li>
     *         <li>1-byte - major version, forward compatible changes only</li>
     *         <li>2-byte - minor version, backward and forward compatible changes</li>
     *         <li>3-byte - storing algorithm: word = <b>(AA BB CC DD)2<b/>:</li>
     *         <ul>
     *             <li>DD = 00, non-fixed array, storing keys and offsets in different files</li>
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

    private final BitSet loaded;
    private final Object[] data;
    private final TypeDescriptor<Type> type;
    private final CachedFile storage;
    private final Configuration configuration;
    private final ChunkQueue queue;

    private FixedLazyArray(TypeDescriptor<Type> type, Object[] data, BitSet loaded, Configuration configuration) {
        this.type = type;
        this.data = data;
        this.loaded = loaded;
        this.configuration = configuration;
        this.storage = new CachedFile(configuration.file());
        this.queue = new ChunkQueue(configuration.write().size());
    }

    public FixedLazyArray(int length, TypeDescriptor<Type> type, Configuration configuration) {
        this(type, new Object[length], loaded(length, length), configuration);
        serialise();
    }

    @SuppressWarnings("unchecked")
    public final Type
    get(int index) {
        assert index >= 0 && index < data.length;

        if (!loaded.get(index)) deserialize(index, configuration.read().chunked() ? configuration.read().size() : 1);
        return (Type) data[index];
    }

    public final void
    set(int index, Type value) {
        assert index >= 0 && index < data.length;

        data[index] = value;
        loaded.set(index);
        if (configuration.write().chunked()) {
            if (queue.add(index)) {
                flush();
            }
        } else serialise(index, 1);
    }

    public int
    length() {
        return data.length;
    }

    public final void
    unload(int index) {
        assert index >= 0 && index < data.length;

        loaded.set(index, false);
        data[index] = null;
    }

    public final void
    unload(int from, int to) {
        assert from >= 0 && from <= to && to < data.length;

        loaded.set(from, to, false);
        Arrays.fill(data, from, to, null);
    }

    public final void
    unload() {
        loaded.clear();
        Arrays.fill(data, null);
    }

    @SuppressWarnings("unchecked")
    public void
    serialise() {
        try (OutputStream output = Files.newOutputStream(configuration.file().path());
             OutputWriter writer = new OutputWriter(output, configuration.file().buffer(), false)) {
            writer.write(HEADER);
            byte[] version = Arrays.copyOf(VERSION, 4);
            version[3] = 0b0_00_00_01;
            writer.write(version);
            Serializer<Integer> integer = SerializerStorage.INTEGER;
            writer.write(integer.serialize(data.length));
            Serializer<Type> serializer = SerializerStorage.get(type);
            writer.write(integer.serialize(serializer.descriptor().size()));
            for (Object datum : data) writer.write(serializer.serialize((Type) datum));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void
    serialise(int index, int size) {
        assert index >= 0 && index < data.length && size > 0;

        try (storage; OutputWriter writer = new OutputWriter(storage.get(),
                Math.min(configuration.file().buffer(), size * type.size()))) {
            seek(storage.peek(), index);
            for (int offset = index; offset < Math.min(index + size, data.length); offset++)
                writer.write(SerializerStorage.get(type).serialize((Type) data[offset]));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void
    serialiseChunk() {
        try (storage; OutputWriter writer = new OutputWriter(storage.get(), configuration.file().buffer())) {
            if (queue.count == 0) return;
            int prior = queue.set.nextSetBit(0), current = 0;
            seek(storage.peek(), prior);
            writer.write(SerializerStorage.get(type).serialize((Type) data[prior]));
            while ((current = queue.set.nextSetBit(prior + 1)) != -1) {
                if (current != prior + 1) {
                    writer.flush();
                    seek(storage.peek(), current);
                }
                writer.write(SerializerStorage.get(type).serialize((Type) data[prior = current]));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void
    deserialize(int index, int size) {
        assert index >= 0 && index < data.length && size >= 0;

        try (storage) {
            RandomAccessFile raf = storage.get();
            InputStream fis = new BufferedInputStream(new FileInputStream(raf.getFD()), configuration.file().buffer());
            seek(raf, index);
            byte[] datum = new byte[SerializerStorage.get(type).descriptor().size()];
            for (int offset = index; offset < Math.min(index + size, data.length); offset++) {
                if (loaded.get(offset)) continue;
                int ignore = fis.read(datum);
                assert ignore == datum.length;
                data[offset] = SerializerStorage.get(type).deserialize(datum);
                loaded.set(offset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <Type extends FixedSizeObject> FixedLazyArray<Type>
    deserialize(TypeDescriptor<Type> type, Configuration configuration, boolean lazy) {
        assert type != null && configuration != null;

        try (InputStream input = new BufferedInputStream(Files.newInputStream(configuration.file().path()))) {
            Serializer<Integer> integer = SerializerStorage.INTEGER;
            byte[] header = new byte[HEADER.length + VERSION.length + (2 * integer.descriptor().size())];
            int ignore = input.read(header), loaded = 0;
            assert ignore == header.length;
            validHeader(header);
            int length = integer.deserialize(header, header.length - 2 * integer.descriptor().size());
            Serializer<Type> serializer = SerializerStorage.get(type);
            int size = serializer.descriptor().size();
            byte[] datum = new byte[size];
            Object[] data = new Object[length];
            if (!lazy)
                for (int index = 0; index < length; index++, loaded++) {
                    ignore = input.read(datum);
                    assert ignore == size;
                    data[index] = serializer.deserialize(datum);
                }
            return new FixedLazyArray<>(type, data, loaded(length, loaded), configuration);
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

    private static void
    validHeader(byte[] header) {
        if (compareVersions(header, HEADER.length) < 0) throw new RuntimeException("Incompatible versions: %s =!= %s"
                .formatted(printVersion(VERSION, 0), printVersion(header, HEADER.length)));
    }

    private void seek(RandomAccessFile raf, int index) throws IOException {
        raf.seek((long) index * SerializerStorage.get(type).descriptor().size() + HEADER.length + VERSION.length + (2L * SerializerStorage.INTEGER.descriptor().size()));
    }

    private static String
    printVersion(byte[] version, int offset) {
        return String.format("%d.%d.%d", version[offset], version[offset + 1], version[offset + 2]);
    }

    private static BitSet
    loaded(int length, int size) {
        BitSet loaded = new BitSet(length);
        loaded.set(0, size, true);
        return loaded;
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

    private static class ChunkQueue {
        private final BitSet set;
        private final int capacity;
        private int count = 0;

        public ChunkQueue(int capacity) {
            this.capacity = capacity;
            this.set = new BitSet(capacity);
        }

        boolean
        add(int index) {
            if (!set.get(index)) {
                set.set(index);
                count ++;
            }
            return count == capacity;
        }

        void
        clear() {
            this.set.clear();
            this.count = 0;
        }

    }

}
