package com.yokalona.array.lazy;

import com.yokalona.array.lazy.Configuration.File;
import com.yokalona.array.lazy.serializers.IntegerSerializer;
import com.yokalona.array.lazy.serializers.Serializer;
import com.yokalona.array.lazy.serializers.SerializerStorage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static com.yokalona.array.lazy.Configuration.Chunked.chunked;
import static com.yokalona.array.lazy.Configuration.Chunked.linear;
import static com.yokalona.array.lazy.Configuration.File.Mode.*;
import static com.yokalona.array.lazy.Configuration.configure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FixedLazyArrayTest {

    @BeforeAll
    public static void setUp() throws IOException {
        SerializerStorage.register(INT.class, new Serializer<>() {
            @Override
            public byte[]
            serialize(INT anInt) {
                return IntegerSerializer.get.serialize(anInt == null ? null : anInt.value);
            }

            @Override
            public INT
            deserialize(byte[] bytes) {
                return new INT(IntegerSerializer.get.deserialize(bytes));
            }

            @Override
            public INT
            deserialize(byte[] bytes, int offset) {
                Integer deserialize = IntegerSerializer.get.deserialize(bytes, offset);
                return deserialize == null ? null : new INT(deserialize);
            }

            @Override
            public int
            sizeOf() {
                return IntegerSerializer.get.sizeOf();
            }
        });
    }

    @Test
    public void testCreateNew() throws IOException {
        Path path = Files.createTempDirectory("fixed");
        try(FixedLazyArray<INT> array = new FixedLazyArray<>(10, INT.class,
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
        try(FixedLazyArray<INT> array = new FixedLazyArray<>(10, INT.class,
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
        try(FixedLazyArray<INT> array = new FixedLazyArray<>(10, INT.class,
                configure(new File(path.resolve("create-new-test.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < 10; i ++) array.set(i, new INT(i), true);
            array.unload();
            for (int i = 0; i < 10; i ++) assertEquals(i, array.get(i).value);
        }
    }

    @Test
    public void testStore() throws IOException {
        int size = 10_000_000;
        Path path = Files.createTempDirectory("fixed");
        try (FixedLazyArray<INT> array = new FixedLazyArray<>(size, INT.class,
                configure(new File(path.resolve("test_fixed.la"), RW, 8192, true))
                        .read(chunked(1000))
                        .write(chunked(10)))) {
            for (int i = 0; i < size; i += 10) {
                for (int j = Math.min(i + 9, size); j > i; j--) {
                    array.set(j, new INT(j), false);
                }
                array.set(i, new INT(i), true);
                array.unload(i, Math.min(i + 9, size));
            }

            array.unload();
            array.deserialize(0, size);

            for (int i = 0; i < size; i++) {
                assertEquals(i, array.get(i).value);
                array.unload(i);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (FixedLazyArray<INT> array = FixedLazyArray.deserialize(INT.class,
                configure(new File(path.resolve("test_fixed.la"), R, 8192, true))
                        .read(chunked(1000))
                        .write(linear()), true)) {
            assertEquals(size, array.length());
            for (int i = 0; i < size; i++) {
                assertEquals(i, array.get(i).value);
                array.unload(i);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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