package com.yokalona.array.lazy;

import com.yokalona.array.lazy.serializers.IntegerSerializer;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.BitSet;

public class FixedLazyArray<Type extends FixedSizeObject>
        implements AutoCloseable {

    private static final byte[] HEADER = new byte[]{-34, -54, -38, -6, -54, -38};
    /**
     * Version mark, represented as 4-byte word.
     * 0-byte - critical version, no backward and no forward compatibility
     * 1-byte - major version, forward compatible changes only
     * 2-byte - minor version, backward and forward compatible changes
     * 3-byte - type of data array:
     * 0 - general lazy array represented as separated offset array and data storage container
     * 1 - fixed size lazy array as a set of padded(meaning always having same size) always persistent object
     */
    private static final byte[] VERSION = new byte[]{0x01, 0x01, 0x00};

    private final BitSet loaded;
    private final Object[] data;
    private final Class<Type> type;
    private final CachedFile storage;
    private final Configuration configuration;

    private FixedLazyArray(Class<Type> type, Object[] data, BitSet loaded, Configuration configuration) {
        this.type = type;
        this.data = data;
        this.loaded = loaded;
        this.configuration = configuration;
        this.storage = new CachedFile(configuration.file());
    }

    public FixedLazyArray(int length, Class<Type> type, Configuration configuration) {
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
    set(int index, Type value, boolean store) {
        assert index >= 0 && index < data.length;

        data[index] = value;
        loaded.set(index);
        if (store) serialise(index, configuration.write().chunked() ? configuration.write().size() : 1);
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
        try (storage; OutputWriter writer = new OutputWriter(storage.get(), configuration.file().buffer())) {
            writer.write(HEADER);
            writer.write(VERSION);
            Serializer<Integer> integer = IntegerSerializer.get;
            writer.write(integer.serialize(data.length));
            Serializer<Type> serializer = SerializerStorage.get(type);
            writer.write(integer.serialize(serializer.sizeOf()));
            for (Object datum : data) writer.write(serializer.serialize((Type) datum));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void
    serialise(int index, int size) {
        assert index >= 0 && index < data.length && size > 0;

        try (storage; OutputWriter writer = new OutputWriter(storage.get(), configuration.file().buffer())) {
            seek(storage.peek(), index);
            for (int offset = index; offset < Math.min(index + size, data.length); offset++)
                writer.write(SerializerStorage.get(type).serialize((Type) data[offset]));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void
    deserialize(int index, int size) {
        assert index >= 0 && index < data.length && size >= 0;

        try (storage) {
            RandomAccessFile raf = storage.get();
            InputStream fis = new BufferedInputStream(new FileInputStream(raf.getFD()), configuration.file().buffer());
            seek(raf, index);
            byte[] datum = new byte[SerializerStorage.get(type).sizeOf()];
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
    deserialize(Class<Type> type, Configuration configuration, boolean lazy) {
        assert type != null && configuration != null;

        try (InputStream input = new BufferedInputStream(Files.newInputStream(configuration.file().path()))) {
            Serializer<Integer> integer = IntegerSerializer.get;
            byte[] header = new byte[HEADER.length + VERSION.length + (2 * integer.sizeOf())];
            validHeader(header);
            int ignore = input.read(header), loaded = 0;
            int length = integer.deserialize(header, header.length - 2 * integer.sizeOf());
            Serializer<Type> serializer = SerializerStorage.get(type);
            int size = serializer.sizeOf();
            byte[] datum = new byte[size];
            Object[] data = new Object[length];
            if (!lazy)
                for (int index = 0; index < length; index++, loaded++) {
                    ignore = input.read(datum);
                    data[index] = serializer.deserialize(datum);
                }
            return new FixedLazyArray<>(type, data, loaded(length, loaded), configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int
    compareVersions(byte[] ignore) {
        return 0;
    }

    private static void
    validHeader(byte[] header) {
        if (compareVersions(header) < 0) throw new RuntimeException("Incompatible versions: %s =!= %s"
                .formatted(printVersion(VERSION), printVersion(header)));
    }

    private void seek(RandomAccessFile raf, int index) throws IOException {
        raf.seek((long) index * SerializerStorage.get(type).sizeOf() + HEADER.length + VERSION.length + Integer.BYTES + Integer.BYTES + 2);
    }

    private static String
    printVersion(byte[] version) {
        return String.format("%d.%d.%d.%d", version[0], version[1], version[2], version[3]);
    }

    private static BitSet
    loaded(int length, int size) {
        BitSet loaded = new BitSet(length);
        loaded.set(0, size, true);
        return loaded;
    }

    @Override
    public void close() {
        storage.closeFile();
    }

}
