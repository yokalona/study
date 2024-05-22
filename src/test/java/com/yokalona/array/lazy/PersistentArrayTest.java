package com.yokalona.array.lazy;

import com.yokalona.array.lazy.configuration.Configuration;
import com.yokalona.array.lazy.subscriber.Subscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.yokalona.array.lazy.CompactInteger.compact;
import static com.yokalona.array.lazy.configuration.Chunked.chunked;
import static com.yokalona.array.lazy.configuration.Chunked.linear;
import static com.yokalona.array.lazy.configuration.File.Mode.*;
import static com.yokalona.array.lazy.configuration.File.file;
import static com.yokalona.array.lazy.configuration.Configuration.InMemory.memorise;
import static com.yokalona.array.lazy.configuration.Configuration.configure;
import static com.yokalona.tree.TestHelper.getSize;
import static com.yokalona.tree.TestHelper.shuffle;
import static org.junit.jupiter.api.Assertions.*;

class PersistentArrayTest {

    public static final int REPEATS = Power.two(5);
    private static final int[] size = {Power.two(5), Power.two(10), Power.two(15)};
    private static final int[] memory = {Power.two(5), Power.two(10)};
    private static final int[] read = {Power.two(5), Power.two(10)};
    private static final int[] write = {Power.two(5), Power.two(10)};

    private Path path;

    @BeforeEach
    public void
    setUp() throws IOException {
        this.path = Files.createTempDirectory("array");
    }

    @Test
    public void testRecordWontPushoutOfChunkIfNotExceededQuota() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("lrlwlm.la")).mode(RWD).cached())
                .memory(memorise(100))
                .addSubscriber(subscriber)
                .read(linear())
                .write(linear());
        try (var array = new PersistentArray<>(10, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index = 0; index < 10; index++) {
                array.set(index, compact(index));
                assertTrue(subscriber.unload.isEmpty());
            }
        }
        assertFalse(subscriber.unload.isEmpty());
    }

    @Test
    public void testSameRecordWontPushoutOfChunkAsItCountAsOneOperation() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("lrlwlm.la")).mode(RWD).cached())
                .memory(memorise(1))
                .addSubscriber(subscriber)
                .read(linear())
                .write(linear());
        try (var array = new PersistentArray<>(10, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index = 0; index < 10; index++) {
                array.set(0, compact(0));
                assertTrue(subscriber.unload.isEmpty());
            }
        }
        assertFalse(subscriber.unload.isEmpty());
    }

    @Test
    public void
    testLinearReadWriteMemory() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("lrlwlm.la")).mode(RWD).cached())
                .memory(memorise(1))
                .addSubscriber(subscriber)
                .read(linear())
                .write(linear());
        try (var array = new PersistentArray<>(100, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index = 0; index < 100; index++) {
                array.set(index, compact(index));
                assertEquals(index, subscriber.load.getLast());
                assertEquals(index, subscriber.serialized.getLast());
                if (index > 0) assertEquals(index - 1, subscriber.unload.getLast());
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            assertTrue(subscriber.deserialized.isEmpty());
            for (int index = 0; index < 100; index++) {
                assertEquals(index, array.get(index).value());
                assertEquals(index, subscriber.load.getLast());
                assertEquals(index, subscriber.deserialized.getLast());
                if (index > 0) assertEquals(index - 1, subscriber.unload.getLast());
            }
        }
    }

    @Test
    public void
    testChunkedReadLinearWriteMemory() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("crlwlm.la")).mode(RWD).cached())
                .memory(memorise(1))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(linear());
        try (var array = new PersistentArray<>(100, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index = 0; index < 100; index++) {
                array.set(index, compact(index));
                assertEquals(index, subscriber.load.getLast());
                assertEquals(index, subscriber.serialized.getLast());
                if (index > 10) assertEquals(index - 10, subscriber.unload.getLast());
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            assertTrue(subscriber.deserialized.isEmpty());
            for (int index = 0; index < 100; index++) {
                assertEquals(index, array.get(index).value());
                if (index % 10 == 0) {
                    for (int i = index; i < Math.min(100, index + 10); i++) {
                        assertEquals(i, subscriber.deserialized.get(i - index));
                    }
                    subscriber.deserialized.clear();
                } else assertTrue(subscriber.deserialized.isEmpty());
            }
        }
    }

    @Test
    public void
    testChunkedReadWriteLinearMemory() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration = configure(
                file(path.resolve("crlwlm.la")).mode(RWD).cached())
                .memory(memorise(1))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(chunked(10));
        try (var array = new PersistentArray<>(100, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index = 0; index < 100; index++) {
                array.set(index, compact(index));
                assertEquals(index, subscriber.load.getLast());
                if ((index + 1) % 10 == 0) {
                    for (int i = 0; i < 10; i++) {
                        assertEquals(i + (index / 10) * 10, subscriber.serialized.get(i));
                    }
                    subscriber.serialized.clear();
                }
                if (index > 10) assertEquals(index - 10, subscriber.unload.getLast());
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            assertTrue(subscriber.deserialized.isEmpty());
            for (int index = 0; index < 100; index++) {
                assertEquals(index, array.get(index).value());
                if (index % 10 == 0) {
                    for (int i = index; i < Math.min(100, index + 10); i++) {
                        assertEquals(i, subscriber.deserialized.get(i - index));
                    }
                    subscriber.deserialized.clear();
                } else assertTrue(subscriber.deserialized.isEmpty());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("generate")
    public void
    testRabbitAndTheHat(PersistenceTest test) throws IOException {
        for (int i = 0; i < REPEATS; i++) {
            Configuration configuration = configure(
                    file(path.resolve("rabbitAndTheHat.la"))
                            .mode(RW)
                            .buffer(Power.two(13))
                            .cached())
                    .memory(memorise(test.memory))
                    .read(test.read > 0 ? chunked(test.read) : linear())
                    .write(test.write > 0 ? chunked(test.write) : linear());
            long lwrite = writeLinear(test.size, configuration);
            long rwrite = writeRandom(test.size, configuration);
            long fileSize = Files.size(configuration.file().path());
            long lread = readLinear(test.size, configuration);
            long rread = readRandom(test.size, configuration);
            printResults(true, i, test, lwrite, rwrite, fileSize, lread, rread);
        }
    }

    @Test
    public void
    testMerge() {
        TestSubscriber subscriber = new TestSubscriber();
        Configuration configuration1 = configure(
                file(path.resolve("merge1.la")).mode(RWD).cached())
                .memory(memorise(1))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(chunked(10));
        Configuration configuration2 = configure(
                file(path.resolve("merge2.la")).mode(RWD).cached())
                .memory(memorise(1))
                .addSubscriber(subscriber)
                .read(chunked(10))
                .write(chunked(10));
        writeLinear(1000, configuration1);
        try (var array1 = PersistentArray.deserialize(CompactInteger.descriptor, configuration1);
             var array2 = new PersistentArray<>(1000, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration2)) {
            for (int index = 0; index < array2.length(); index++) assertNull(array2.get(index));
            PersistentArray.copy(array1, 0, array2, 0, 1000);
            for (int index = 0; index < array2.length(); index++) assertEquals(index, array2.get(index).value());
        }
    }

    private static long
    readLinear(int length, Configuration configuration) {
        long start = System.currentTimeMillis();
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            for (int index = 0; index < length; index++)
                assertEquals(index, array.get(index).value());
        }
        return System.currentTimeMillis() - start;
    }

    private static long
    readRandom(int length, Configuration configuration) {
        int[] indices = new int[length];
        for (int index = 0; index < length; index++) {
            indices[index] = index;
        }
        shuffle(indices);
        long start = System.currentTimeMillis();
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            for (int index : indices) {
                CompactInteger value = array.get(index);
                if (value == null || index != value.value()) {
                    System.out.println(index + " " + Arrays.toString(indices));
                }
                assertNotNull(value);
                assertEquals(index, value.value());
            }
        }
        return System.currentTimeMillis() - start;
    }

    private static long
    writeLinear(int length, Configuration configuration) {
        long start = System.currentTimeMillis();
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index = 0; index < length; index++) array.set(index, new CompactInteger(index));
        }
        return System.currentTimeMillis() - start;
    }

    private static long
    writeRandom(int length, Configuration configuration) {
        int[] indices = new int[length];
        for (int index = 0; index < length; index++) {
            indices[index] = index;
        }
        shuffle(indices);
        long start = System.currentTimeMillis();
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, new PersistentArray.FixedObjectLayout(CompactInteger.descriptor), configuration)) {
            for (int index : indices) array.set(index, new CompactInteger(index));
        }
        return System.currentTimeMillis() - start;
    }

    private static void
    printResults(boolean print, int repeat, PersistenceTest test, long lwrite, long rwrite, long fileSize,
                 long lread, long rread) {
        if (!print) return;
        System.out.printf("""
                        |--------------------------------------------------------------|
                        | Progress:                               | %5d/%-5d repeat |
                        |--------------------------------------------------------------|
                        | Array size:                             | %10d records |
                        |     Memory size:                        | %10d records |
                        |     Chunks:                             | %10s         |
                        |         Read:                           | %10d records |
                        |         Write:                          | %10d records |
                        |     Took:                               | %10s         |
                        |         Linear write                    | %10d ms      |
                        |         Random write                    | %10d ms      |
                        |         Linear read:                    | %10d ms      |
                        |         Random read:                    | %10d ms      |
                        |     File size:                          | %13s      |%n""",
                repeat, PersistentArrayTest.REPEATS, test.size, test.memory, ' ', test.read, test.write, ' ', lwrite, rwrite, lread, rread, getSize(fileSize));
    }

    record PersistenceTest(int size, int memory, int read, int write) {
    }

    public static List<PersistenceTest>
    generate() {
        List<PersistenceTest> generated = new ArrayList<>(size.length * memory.length * read.length * write.length);
        for (int size : size)
            for (int memory : memory)
                for (int read : read)
                    for (int write : write) generated.add(new PersistenceTest(size, memory, read, write));
        return generated;
    }

    static class TestSubscriber implements Subscriber {
        List<Integer> serialized = new ArrayList<>();
        List<Integer> deserialized = new ArrayList<>();
        List<Integer> load = new ArrayList<>();
        List<Integer> unload = new ArrayList<>();

        @Override
        public void onSerialized(int index) {
            serialized.add(index);
        }

        @Override
        public void onDeserialized(int index) {
            deserialized.add(index);
        }

        @Override
        public void onLoad(int index) {
            load.add(index);
        }

        @Override
        public void onUnload(int index) {
            unload.add(index);
        }
    }
}