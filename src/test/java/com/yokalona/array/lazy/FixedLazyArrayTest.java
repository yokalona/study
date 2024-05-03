package com.yokalona.array.lazy;

import com.yokalona.array.lazy.Configuration.File;
import com.yokalona.array.lazy.serializers.IntegerSerializer;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Random;

import static com.yokalona.array.lazy.Configuration.Chunked.chunked;
import static com.yokalona.array.lazy.Configuration.Chunked.linear;
import static com.yokalona.array.lazy.Configuration.File.Mode.*;
import static com.yokalona.array.lazy.Configuration.configure;
import static org.junit.jupiter.api.Assertions.*;

class FixedLazyArrayTest {

    public static final Random RANDOM = new Random();
    static TypeDescriptor<INT> in = new TypeDescriptor<>(5, INT.class);
    static TypeDescriptor<VARCHAR> varchar256 = new TypeDescriptor<>(256 + 1 + SerializerStorage.INTEGER.descriptor().size(), VARCHAR.class);

    @BeforeAll
    public static void setUp() {
        SerializerStorage.register(varchar256, new Serializer<>() {

            @Override
            public byte[] serialize(VARCHAR value) {
                byte[] bytes = new byte[descriptor().size()];
                if (value == null) {
                    bytes[0] = 0xF;
                    return bytes;
                }
                byte[] string = value.value.getBytes(StandardCharsets.UTF_8);
                byte[] length = SerializerStorage.INTEGER.serialize(string.length);
                System.arraycopy(length, 0, bytes, 1, length.length);
                System.arraycopy(string, 0, bytes, 1 + length.length, Math.min(string.length, bytes.length - 1 - length.length));
                return bytes;
            }

            @Override
            public VARCHAR deserialize(byte[] bytes) {
                return deserialize(bytes, 0);
            }

            @Override
            public VARCHAR deserialize(byte[] bytes, int offset) {
                if (bytes[offset] == 0xF) return null;
                Integer length = SerializerStorage.INTEGER.deserialize(bytes, offset + 1);
                return new VARCHAR(new String(bytes, offset + 1 + SerializerStorage.INTEGER.descriptor().size(), length),
                        descriptor().size());
            }

            @Override
            public TypeDescriptor<VARCHAR> descriptor() {
                return varchar256;
            }
        });

        SerializerStorage.register(in, new Serializer<>() {

            Serializer<Integer> serializer = SerializerStorage.INTEGER;

            @Override
            public byte[]
            serialize(INT anInt) {
                return serializer.serialize(anInt == null ? null : anInt.value);
            }

            @Override
            public INT
            deserialize(byte[] bytes) {
                return new INT(serializer.deserialize(bytes));
            }

            @Override
            public INT
            deserialize(byte[] bytes, int offset) {
                Integer deserialize = serializer.deserialize(bytes, offset);
                return deserialize == null ? null : new INT(deserialize);
            }

            @Override
            public TypeDescriptor<INT> descriptor() {
                return new TypeDescriptor<>(5, INT.class);
            }
        });
    }

    @Test
    public void testCreateNew() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try(FixedLazyArray<INT> array = new FixedLazyArray<>(10, in,
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i ++) array.set(i, new INT(i), false);
            for (int i = 0; i < 10; i ++) assertEquals(i, array.get(i).value);
        }
    }

    @Test
    public void testCreateNewPersistent() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try(FixedLazyArray<INT> array = new FixedLazyArray<>(10, in,
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i ++) array.set(i, new INT(i), true);
            for (int i = 0; i < 10; i ++) assertEquals(i, array.get(i).value);
        }
    }

    @Test
    public void testCreateNewPersistentMemoryFree() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try(FixedLazyArray<INT> array = new FixedLazyArray<>(10, in,
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(linear()))) {
            for (int i = 0; i < 10; i ++) array.set(i, new INT(i), true);
            array.unload();
            for (int i = 0; i < 10; i ++) assertEquals(i, array.get(i).value);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000, 10000, 100000, 1000000})
    public void testLimitedString(int size) throws IOException {
        Path path = Files.createTempDirectory("fixed");
        byte[] bytes = new byte[varchar256.size()];
        Path file = path.resolve("string.la");
        try (FixedLazyArray<VARCHAR> array = new FixedLazyArray<>(size, varchar256,
                configure(new File(file, RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(2)))) {
            for (int i = 0; i < size; i ++) {
                RANDOM.nextBytes(bytes);
                array.set(i, new VARCHAR(new String(bytes, StandardCharsets.UTF_8), varchar256.size()), true);
                array.unload(i);
            }
            assertNotNull(array.get(2));
            System.out.println(getSize(Files.size(file)));
        }
    }

    static long kilo = 1024;
    static long mega = kilo * kilo;
    static long giga = mega * kilo;
    static long tera = giga * kilo;
    public static String getSize(long size) {
        String s = "";
        double kb = (double)size / kilo;
        double mb = kb / kilo;
        double gb = mb / kilo;
        double tb = gb / kilo;
        if(size < kilo) {
            s = size + " Bytes";
        } else if(size < mega) {
            s =  String.format("%.2f", kb) + " KB";
        } else if(size < giga) {
            s = String.format("%.2f", mb) + " MB";
        } else if(size < tera) {
            s = String.format("%.2f", gb) + " GB";
        } else {
            s = String.format("%.2f", tb) + " TB";
        }
        return s;
    }

    @Test
    public void testStore() throws IOException {
        int size = 10_000_000;
        Path path = Files.createTempDirectory("fixed");
        try (FixedLazyArray<INT> array = new FixedLazyArray<>(size, in,
                configure(new File(path.resolve("test_fixed.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(1000)))) {
            for (int i = 0; i < size; i ++) {
                array.set(i, new INT(i), true);
            }
            array.flush();
            array.unload();

            for (int i = 0; i < size; i++) {
                assertEquals(i, array.get(i).value);
                array.unload(i);
            }
        }

        try (FixedLazyArray<INT> array = FixedLazyArray.deserialize(in,
                configure(new File(path.resolve("test_fixed.la"), R, 8192, true))
                        .read(chunked(1000))
                        .write(linear()), true)) {
            assertEquals(size, array.length());
            for (int i = 0; i < size; i++) {
                assertEquals(i, array.get(i).value);
                array.unload(i);
            }
        }
    }

    static class VARCHAR implements FixedSizeObject {

        private final String value;

        private final int size;

        VARCHAR(String value, int size) {
            this.value = value;
            this.size = size;
        }

        public String
        get() {
            return value;
        }

        @Override
        public int sizeOf() {
            return size;
        }
    }

    static class INT implements FixedSizeObject {

        private final Integer value;

        INT(Integer value) {
            this.value = value;
        }

        public Integer
        get() {
            return value;
        }

        @Override
        public int sizeOf() {
            return Integer.BYTES;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            INT anInt = (INT) o;

            return Objects.equals(value, anInt.value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }
    }

}