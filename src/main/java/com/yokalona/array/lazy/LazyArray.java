package com.yokalona.array.lazy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.yokalona.tree.b.FileConfiguration;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public class LazyArray<Type> implements Iterable<Type> {
    final Class<Type> type;
    final Object[] data;
    private final BitSet loaded;
    final FileConfiguration file;

    /**
     * Version mark, represented as 4-byte word.
     * 0-byte - critical version, no backward and no forward compatibility
     * 1-byte - major version, forward compatible changes only
     * 2-byte - minor version, backward and forward compatible changes
     * 3-byte - type of data array:
     *   0 - general lazy array represented as separated offset array and data storage container
     */
    // TODO: DECADAFACADA
    private static final byte[] VERSION = new byte[] { 0x00, 0x01, 0x00, 0x00 };

    LazyArray(FileConfiguration file, Class<Type> type, Object[] data, BitSet loaded) {
        this.file = file;
        this.type = type;
        this.data = data;
        this.loaded = loaded;
    }

    public LazyArray(int length, FileConfiguration file, Class<Type> type) {
        this(file, type, new Object[length], loaded(length, length));
    }

    @SuppressWarnings("unchecked")
    public final Type
    get(int index) {
        if (! loaded.get(index)) data[index] = deserialize(index);
        return (Type) data[index];
    }

    @SuppressWarnings("unchecked")
    public final Type
    get(int index, boolean load) {
        if (load) return get(index);
        return (Type) data[index];
    }

    public final void
    set(int index, Type value) {
        data[index] = value;
        loaded.set(index);
    }

    private long[]
    readOffsets() {
        try (Input input = new Input(Files.newInputStream(Paths.get(file.offset())))) {
            byte[] version = new byte[4];
            int length = input.readInt(true);
            input.read(version, 0, length - 1);
            if (compareVersions(version) >= 0) return file.kryo().readObject(input, long[].class);
            else throw new RuntimeException("Incompatible versions: " + printVersion(VERSION) + " is not compatible with " + printVersion(version));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void
    serialize() {
        try (Output storage = new Output(new FileOutputStream(file.data()));
             Output offsets = new Output(new FileOutputStream(file.offset()))) {
            file.kryo().writeObject(storage, VERSION);
            file.kryo().writeObject(storage, file.offset());
            file.kryo().writeObject(storage, data.length);
            long[] offset = new long[data.length];
            for (int index = 0; index < data.length; index++) {
                offset[index] = storage.total();
                file.kryo().writeObjectOrNull(storage, data[index], type);
            }
            file.kryo().writeObject(offsets, VERSION);
            file.kryo().writeObject(offsets, offset);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Type
    deserialize(int index) {
        long[] offsets = readOffsets();
        long offset = offsets[index];
        int size = 1024;
        byte[] buffer = new byte[size];

        try (Input input = new Input(new FileInputStream(file.data()))) {
            while (offset > size)
                offset -= input.read(buffer, 0, size);
            int ignore = input.read(buffer, 0, (int) offset);
            return file.kryo().readObjectOrNull(input, type);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static String
    printVersion(byte[] version) {
        return String.format("%d.%d.%d.%d", version[0], version[1], version[2], version[3]);
    }

    public static int compareVersions(byte[] version) {
        return 0;
    }

    public final void
    unload(int index) {
        loaded.set(index, false);
        data[index] = null;
    }

    public final void
    unload(int from, int to) {
        loaded.set(from, to, false);
        Arrays.fill(data, from, to, null);
    }

    public final void
    unload() {
        loaded.clear();
        Arrays.fill(data, null);
    }

    public final int
    length() {
        return data.length;
    }

    @Override
    public Iterator<Type> iterator() {
        return IntStream.range(0, data.length)
                .boxed()
                .map(index -> get(index, false))
                .iterator();
    }

    public static <Type> LazyArray<Type>
    deserialize(String file, Class<Type> type, Kryo kryo) {
        try (Input input = new Input(new FileInputStream(file))) {
            byte[] version = kryo.readObject(input, byte[].class);
            if (compareVersions(version) >= 0) {
                String offsets = kryo.readObject(input, String.class);
                int length = kryo.readObject(input, Integer.class);
                Object[] data = new Object[length];
                for (int index = 0; index < length; index ++)
                    data[index] = kryo.readObjectOrNull(input, type);
                return new LazyArray<>(new FileConfiguration(file, offsets, kryo), type, data, loaded(length, length));
            } else throw new RuntimeException("Incompatible versions: " + printVersion(VERSION) + " is not compatible with " + printVersion(version));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    static BitSet
    loaded(int length, int size) {
        BitSet loaded = new BitSet(length);
        loaded.set(0, size, true);
        return loaded;
    }

}
