package com.yokalona.tree.b;

import com.esotericsoftware.kryo.Kryo;
import com.yokalona.array.lazy.LazyArray;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LazyArrayTest {

    // File format:
    // 1. [OBJECT-0][OBJECT-1]...[OBJECT-N]
    //    [OFFSET-0][OFFSET-1]...[OFFSET-N]
    // 2. [LENGTH][OBJECT-0][PADDING][OBJECT-1][PADDING]...[OBJECT-N][PADDING]
    // 3. [OBJECT-N/2][OBJECT-N/4][OBJECT-N*3/4]...

    public static final String FILE_1 = "test_lazy.la";
    private static final Kryo kryo = new Kryo();
    private static final FileConfiguration file = new FileConfiguration(FILE_1, FILE_1 + 'o', kryo);

    @BeforeAll
    public static void setUp() {
        kryo.register(long[].class);
        kryo.register(byte[].class);
        kryo.register(Integer[].class);
    }

    @AfterAll
    public static void tearDown() throws IOException {
        Files.deleteIfExists(Path.of(file.data()));
        Files.deleteIfExists(Path.of(file.offset()));
    }

    @Test
    public void testCreate() {
        int size = 1_000;
        LazyArray<Integer> inmemory = new LazyArray<>(size, file, Integer.class);

        for (int sample = 0; sample < size; sample ++) {
            inmemory.set(sample, sample);
        }

        for (int sample = 0; sample < size; sample ++) {
            assertEquals(sample, inmemory.get(sample));
        }
    }

    @Test
    public void testStore() {
        int size = 1_000;
        LazyArray<Integer> inmemory = new LazyArray<>(size, file, Integer.class);

        for (int sample = 0; sample < size; sample ++) {
            inmemory.set(sample, sample + 1);
        }

        for (int sample = 0; sample < size; sample ++) {
            assertEquals(sample + 1, inmemory.get(sample));
        }

        inmemory.serialize();

        LazyArray<?> loaded = LazyArray.deserialize(FILE_1, Integer.class, kryo);
        for (int sample = 0; sample < size; sample ++) {
            assertEquals(inmemory.get(sample), loaded.get(sample, false));
        }
    }

    @Test
    public void testUnload() {
        int size = 1_000;
        LazyArray<Integer> inmemory = new LazyArray<>(size, file, Integer.class);

        for (int sample = 0; sample < size; sample ++) {
            inmemory.set(sample, sample);
        }

        for (int sample = 0; sample < size; sample ++) {
            assertEquals(sample, inmemory.get(sample));
        }

        inmemory.serialize();

        for (int sample = 0; sample < size; sample ++) {
            inmemory.unload(sample);
        }

        for (int sample = 0; sample < size; sample ++) {
            assertNull(inmemory.get(sample, false));
        }

        for (int sample = 0; sample < size; sample ++) {
            assertEquals(sample, inmemory.get(sample));
        }
    }

}