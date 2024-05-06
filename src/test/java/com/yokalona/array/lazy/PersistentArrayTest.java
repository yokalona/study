package com.yokalona.array.lazy;

import com.yokalona.array.lazy.Configuration.File;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.yokalona.array.lazy.Configuration.Chunked.chunked;
import static com.yokalona.array.lazy.Configuration.Chunked.linear;
import static com.yokalona.array.lazy.Configuration.File.Mode.*;
import static com.yokalona.array.lazy.Configuration.configure;
import static org.junit.jupiter.api.Assertions.*;

class PersistentArrayTest {

    public static final Random RANDOM = new Random();
    static TypeDescriptor<INT> in = new TypeDescriptor<>(5, INT.class);
    static TypeDescriptor<CompactInteger> compact = new TypeDescriptor<>(5, CompactInteger.class);
    static TypeDescriptor<VARCHAR> varchar256 = new TypeDescriptor<>(256 + 1 + SerializerStorage.INTEGER.descriptor().size(), VARCHAR.class);

    @BeforeAll
    public static void setUp() {
        byte [] bytes = new byte[5];
        SerializerStorage.register(compact, new Serializer<>() {

            @Override
            public byte[] serialize(CompactInteger value) {
                if (value == null) {
                    bytes[0] = 0xF;
                    return bytes;
                } else bytes[0] = 0x0;
                int length = bytes.length;
                int vals = value.get();
                for (int i = 0; i < bytes.length - 1; i++) {
                    bytes[length - i - 1] = (byte) (vals & 0xFF);
                    vals >>= 8;
                }
                return bytes;
            }

            @Override
            public CompactInteger deserialize(byte[] bytes) {
                return deserialize(bytes, 0);
            }

            @Override
            public CompactInteger deserialize(byte[] bytes, int offset) {
                if (bytes[offset] == 0xF) return null;
                int value = 0;
                for (int index = offset + 1; index < offset + 5; index++) {
                    value = (value << 8) + (bytes[index] & 0xFF);
                }
                return new CompactInteger(value);
            }

            @Override
            public TypeDescriptor<CompactInteger> descriptor() {
                return compact;
            }
        });

        SerializerStorage.register(varchar256, new Serializer<>() {

            @Override
            public byte[] serialize(VARCHAR value) {
                byte[] bytes = new byte[descriptor().size()];
                if (value == null) {
                    bytes[0] = 0xF;
                    return bytes;
                }
                byte[] string = value.value.getBytes(StandardCharsets.UTF_8);
                byte[] length = SerializerStorage.INTEGER.serialize(Math.min(string.length, descriptor().size() - 1 - SerializerStorage.INTEGER.descriptor().size()));
                System.arraycopy(length, 0, bytes, 1, length.length);
                int actual = Math.min(string.length, bytes.length - 1 - length.length);
                System.arraycopy(string, 0, bytes, 1 + length.length, actual);
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

            final Serializer<Integer> serializer = SerializerStorage.INTEGER;

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
        try (PersistentArray<INT> array = new PersistentArray<>(10, in, new PersistentArray.FixedSearchFormat(in),
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i++) array.set(i, new INT(i));
            for (int i = 0; i < 10; i++) assertEquals(i, array.get(i).value);
        }
    }

    @Test
    public void testCreateNewPersistent() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try (PersistentArray<INT> array = new PersistentArray<>(10, in, new PersistentArray.FixedSearchFormat(in),
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i++) array.set(i, new INT(i));
            for (int i = 0; i < 10; i++) assertEquals(i, array.get(i).value);
        }
    }

    @Test
    public void testCreateNewPersistentMemoryFree() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try (PersistentArray<INT> array = new PersistentArray<>(10, in, new PersistentArray.FixedSearchFormat(in),
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(linear()))) {
            for (int i = 0; i < 10; i++) array.set(i, new INT(i));
            array.unload();
            for (int i = 0; i < 10; i++) assertEquals(i, array.get(i).value);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {100_000_000})
    public void test(int size) throws IOException {
        Path path = Files.createTempDirectory("fixed");
        Path file = path.resolve("compact.la");
        try (PersistentArray<CompactInteger> array = new PersistentArray<>(size, compact, new PersistentArray.FixedSearchFormat(compact),
                configure(new File(file, RW, 32768, true))
                        .read(chunked(100000))
                        .write(chunked(100000)))) {
            for (int i = 0; i < size; i++) {
                array.set(i, new CompactInteger(i));
                if (i > 100001) array.unload(i - 100001);
            }
            array.flush();
            array.unload();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {100_000_000})
    public void testIntegers(int size) throws IOException {
        Path path = Files.createTempDirectory("fixed");
        Path file = path.resolve("integers.la");
        try (PersistentArray<Integer> array = new PersistentArray<>(size, SerializerStorage.INTEGER.descriptor(), new PersistentArray.FixedSearchFormat(SerializerStorage.INTEGER.descriptor()),
                configure(new File(file, RW, 32768, true))
                        .read(chunked(100000))
                        .write(chunked(100000)))) {
            for (int i = 0; i < size; i++) {
                array.set(i, i);
                if (i > 100001) array.unload(i - 100001);
            }
            array.flush();
            array.unload();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000, 10000, 100000, 1000000})
    public void testLimitedString(int size) throws IOException {
        Path path = Files.createTempDirectory("fixed");
        Path file = path.resolve("string.la");
        try (PersistentArray<VARCHAR> array = new PersistentArray<>(size, varchar256, new PersistentArray.FixedSearchFormat(varchar256),
                configure(new File(file, RW, 8192, true))
                        .read(linear())
                        .write(linear()))) {
            Map<Integer, String> control = new HashMap<>();
            for (int i = 0; i < size; i++) {
                String value = UUID.randomUUID().toString();
                array.set(i, new VARCHAR(value, 256));
                if (RANDOM.nextBoolean()) {
                    control.put(i, value);
                }
                array.unload(i);
            }
            System.out.println(getSize(Files.size(file)));
            for (Map.Entry<Integer, String> entry : control.entrySet()) {
                assertEquals(entry.getValue(),
                        array.get(entry.getKey()).value, "" + array.get(entry.getKey()).value.length());
                array.unload(entry.getKey());
            }
        }
    }

    static long kilo = 1024;
    static long mega = kilo * kilo;
    static long giga = mega * kilo;
    static long tera = giga * kilo;

    public static String getSize(long size) {
        double kb = (double) size / kilo, mb = kb / kilo, gb = mb / kilo, tb = gb / kilo;
        if (size < kilo) return size + " B";
        else if (size < mega) return String.format("%.2f Kb", kb);
        else if (size < giga) return String.format("%.2f Mb", mb);
        else if (size < tera) return String.format("%.2f Gb", gb);
        else return String.format("%.2f Tb", tb);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000, 10000, 100000, 1000000, 10_000_000})
    public void testStore(int size) throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try (PersistentArray<INT> array = new PersistentArray<>(size, in, new PersistentArray.FixedSearchFormat(in),
                configure(new File(path.resolve("test_fixed.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(linear()))) {
            for (int i = 0; i < size; i++) {
                array.set(i, new INT(i));
                array.unload(i);
            }
        }
        System.out.println(getSize(Files.size(path.resolve("test_fixed.la"))));

        try (PersistentArray<INT> array = PersistentArray.deserialize(in,
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

    static class CompactInteger {
        private final int value;

        CompactInteger(int value) {
            this.value = value;
        }

        public int
        get() {
            return value;
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