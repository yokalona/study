package com.yokalona.array.lazy;

import com.esotericsoftware.kryo.io.Input;
import com.yokalona.array.lazy.serializers.IntegerSerializer;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.tree.b.FileConfiguration;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

public class FixedLazyArray<Type extends FixedSizeObject> extends LazyArray<Type> {
    private static final byte[] HEADER = new byte[]
            {0xD, 0xE, 0xC, 0xA, 0xD, 0xA, 0xF, 0xA, 0xC, 0xA, 0xD, 0xA};
    private static final byte[] VERSION = new byte[]
            {0x01, 0x01, 0x00, 0x01};

    FixedLazyArray(FileConfiguration file, Class<Type> type, Object[] data, BitSet loaded) {
        super(file, type, data, loaded);
    }

    public FixedLazyArray(int length, FileConfiguration file, Class<Type> type) {
        super(length, file, type);
    }

    public void serialise() {
        try (OutputStream output = Files.newOutputStream(Path.of(file.data()));
             OutputWriter writer = new OutputWriter(output, 4096)) {
            writer.write(HEADER);
            writer.write(VERSION);
            Serializer<Integer> integer = IntegerSerializer.INSTANCE;
            writer.write(integer.serialize(data.length));
            Serializer<Type> serializer = SerializerStorage.get(type);
            writer.write(integer.serialize(serializer.sizeOf()));
            for (Object datum : data) {
                writer.write(serializer.serialize((Type) datum));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class OutputWriter implements AutoCloseable {

        private final OutputStream output;
        private final byte[] buffer;
        private int position;

        public OutputWriter(OutputStream output, int size) {
            this.output = output;
            this.buffer = new byte[size];
        }

        public void write(byte[] data) throws IOException {
            int written = 0;
            while (position + data.length - written > buffer.length) {
                written += write(data, written, buffer.length - position);
                flush();
            }
            write(data, written, data.length - written);
        }

        public int write(byte[] data, int offset, int length) {
            System.arraycopy(data, offset, buffer, position, length);
            position += length;
            return length;
        }

        public void flush() throws IOException {
            output.write(buffer, 0, position);
            position = 0;
        }

        @Override
        public void close() throws Exception {
            flush();
            output.close();
        }
    }

    public Type
    deserialize(int index) {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(Path.of(file.data())))) {
            byte[] header = new byte[HEADER.length];
            byte[] version = new byte[VERSION.length];
            int ignore = input.read(header);
            ignore += input.read(version);
            if (ignore != HEADER.length + VERSION.length || compareVersions(version) < 0 || ! validHeader(header)) {
                throw new RuntimeException();
            }
            int length = readInt(input);
            int size = readInt(input);
            long offset = (long) index * size;
            byte[] buffer = new byte[1024];

            while (offset > 1024)
                offset -= input.read(buffer);
            ignore = input.read(buffer, 0, (int) offset);
            Serializer<Type> serializer = SerializerStorage.get(type);
            byte[] datum = new byte[size];
            ignore = input.read(datum);
            return serializer.deserialize(datum);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <Type extends FixedSizeObject> FixedLazyArray<Type>
    deserialize(String file, Class<Type> type) {
        try (InputStream input = new BufferedInputStream(Files.newInputStream(Path.of(file)))) {
            byte[] header = new byte[HEADER.length];
            byte[] version = new byte[VERSION.length];
            int ignore = input.read(header);
            ignore += input.read(version);
            if (ignore != HEADER.length + VERSION.length || compareVersions(version) < 0 || ! validHeader(header)) {
                throw new RuntimeException();
            }
            int length = readInt(input);
            int size = readInt(input);
            byte[] datum = new byte[size];
            Serializer<Type> serializer = SerializerStorage.get(type);
            Object[] data = new Object[length];
            for (int index = 0; index < length; index++) {
                ignore = input.read(datum);
                data[index] = serializer.deserialize(datum);
            }
            return new FixedLazyArray<>(new FileConfiguration(file, file, null), type, data, LazyArray.loaded(length, length));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int
    readInt(InputStream input) throws IOException {
        byte[] length = new byte[Integer.BYTES];
        int ignore = input.read(length);
        return IntegerSerializer.INSTANCE.deserialize(length);
    }

    private static boolean validHeader(byte[] header) {
        return true;
    }
}
