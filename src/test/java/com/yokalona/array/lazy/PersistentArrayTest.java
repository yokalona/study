package com.yokalona.array.lazy;

import com.yokalona.array.lazy.Configuration.File;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import com.yokalona.array.lazy.serializers.TypeDescriptor;
import com.yokalona.tree.TestHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.yokalona.array.lazy.Configuration.Chunked.chunked;
import static com.yokalona.array.lazy.Configuration.Chunked.linear;
import static com.yokalona.array.lazy.Configuration.File.Mode.*;
import static com.yokalona.array.lazy.Configuration.File.file;
import static com.yokalona.array.lazy.Configuration.InMemory.memorise;
import static com.yokalona.array.lazy.Configuration.InMemory.none;
import static com.yokalona.array.lazy.Configuration.configure;
import static org.junit.jupiter.api.Assertions.*;

class PersistentArrayTest {

    public static final Random RANDOM = new Random();
    static TypeDescriptor<INT> in = new TypeDescriptor<>(5, INT.class);
    static TypeDescriptor<CompactInteger> compact = new TypeDescriptor<>(5, CompactInteger.class);
    static TypeDescriptor<VARCHAR> varchar256 = new TypeDescriptor<>(256 + 1 + SerializerStorage.INTEGER.descriptor().size(), VARCHAR.class);

    @BeforeAll
    public static void setUp() {
        byte[] bytes = new byte[5];
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
        try (PersistentArray<INT> array = new PersistentArray<>(10, in, new PersistentArray.FixedObjectLayout(in),
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .memory(none())
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i++) array.set(i, new INT(i));
            for (int i = 0; i < 10; i++) assertEquals(i, array.get(i).value);
        }
    }

    @Test
    public void testCreateNewPersistent() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try (PersistentArray<INT> array = new PersistentArray<>(10, in, new PersistentArray.FixedObjectLayout(in),
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .memory(none())
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i++) array.set(i, new INT(i));
            for (int i = 0; i < 10; i++) assertEquals(i, array.get(i).value);
        }
    }

    static long kilo = 1024;
    static long mega = kilo * kilo;
    static long giga = mega * kilo;
    static long tera = giga * kilo;

    public static String getSize(long size) {
        double kb = (double) size / kilo, mb = kb / kilo, gb = mb / kilo, tb = gb / kilo;
        if (size < kilo) return size + " b";
        else if (size < mega) return String.format("%.2f Kb", kb);
        else if (size < giga) return String.format("%.2f Mb", mb);
        else if (size < tera) return String.format("%.2f Gb", gb);
        else return String.format("%.2f Tb", tb);
    }

    int[] size = {
            Power.two(5), Power.two(10), Power.two(20), Power.two(25)};
    int[] memory = {
            Power.two(5),
            Power.two(10)};
    int[] read = {
            Power.two(5),
            Power.two(10)};
    int[] write = {
            Power.two(5),
            Power.two(10)};

    record PersistenceTest(int size, int memory, int read, int write) { }

    @Test
    public void test222() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        PersistenceTest[] tests = new PersistenceTest[size.length * memory.length * read.length * write.length];
        int t = 0;
        for (int length : size) {
            for (int mem : memory) {
                for (int readChunk : read) {
                    for (int writeChunk : write) {
                        tests[t++] = new PersistenceTest(length, mem, readChunk, writeChunk);
                    }
                }
            }
        }
        System.out.printf("Generated %d tests for linear read/write array%n", tests.length);
        for (PersistenceTest test : tests) {
            Configuration configuration = configure(
                    file(path.resolve("crwmcs.la"))
                            .mode(RW)
                            .buffer(Power.two(13))
                            .cached())
                    .memory(memorise(test.memory))
                    .read(test.read > 0 ? chunked(test.read) : linear())
                    .write(test.write > 0 ? chunked(test.write) : linear());
            long lwrite = createLinear(test.size, configuration);
            long rwrite = createRandom(test.size, configuration);
            long fileSize = Files.size(configuration.file().path());
            long read = readLinear(test.size, configuration);
            printResults(test, lwrite, rwrite, fileSize, read);
        }
    }

    private static long readLinear(int length, Configuration configuration) {
        long start = System.currentTimeMillis();
        try (var array = PersistentArray.deserialize(compact, configuration)) {
            for (int index = 0; index < length; index++)
                assertEquals(index, array.get(index).value);
        }
        return System.currentTimeMillis() - start;
    }

    private static long createLinear(int length, Configuration configuration) {
        long start = System.currentTimeMillis();
        try (var array = new PersistentArray<>(length, compact, new PersistentArray.FixedObjectLayout(compact), configuration)) {
            for (int index = 0; index < length; index++) array.set(index, new CompactInteger(index));
        }
        return System.currentTimeMillis() - start;
    }

    private static long createRandom(int length, Configuration configuration) {
        long start = System.currentTimeMillis();
        int [] indices = new int[length];
        for (int index = 0; index < length; index ++) {
            indices[index] = index;
        }
        TestHelper.shuffle(indices);
        try (var array = new PersistentArray<>(length, compact, new PersistentArray.FixedObjectLayout(compact), configuration)) {
            for (int index : indices) array.set(index, new CompactInteger(index));
        }
        return System.currentTimeMillis() - start;
    }

    private static long readRandom(int length, Configuration configuration, int[] p) {
        long start = System.currentTimeMillis();
        try (var array = PersistentArray.deserialize(compact, configuration)) {
            for (int index = 0; index < length; index++) {
                int idx = RANDOM.nextInt(length);
                assertEquals(p[idx], array.get(idx).value, " " + configuration);
            }
        }
        return System.currentTimeMillis() - start;
    }

    private static void printResults(PersistenceTest test, long lwrite, long rwrite, long fileSize, long reading) {
        System.out.printf("""
                        |--------------------------------------------------------------|
                        | Array size:                             | %10d records |
                        |     Memory size:                        | %10d records |
                        |     Chunks:                             | %10s         |
                        |         Read:                           | %10d records |
                        |         Write:                          | %10d records |
                        |     Took:                               | %10s         |
                        |         Linear write                    | %10d ms      |
                        |         Random write                    | %10d ms      |
                        |         Deserialization and Validation: | %10d ms      |
                        |     File size:                          | %13s      |%n""",
                test.size, test.memory, ' ', test.read, test.write, ' ', lwrite, rwrite, reading, getSize(fileSize));
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompactInteger that = (CompactInteger) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
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