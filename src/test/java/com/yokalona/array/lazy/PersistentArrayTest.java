package com.yokalona.array.lazy;

import com.yokalona.array.lazy.configuration.Configuration;
import com.yokalona.array.lazy.debug.CompactInteger;
import com.yokalona.array.lazy.subscriber.CountingSubscriber;
import com.yokalona.array.lazy.subscriber.CountingSubscriber.Counter;
import com.yokalona.tree.TestHelper.PersistenceTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.yokalona.array.lazy.configuration.Chunked.chunked;
import static com.yokalona.array.lazy.configuration.ChunkedRead.read;
import static com.yokalona.array.lazy.configuration.ChunkedWrite.write;
import static com.yokalona.array.lazy.configuration.File.Mode.*;
import static com.yokalona.array.lazy.configuration.File.file;
import static com.yokalona.array.lazy.configuration.Configuration.configure;
import static com.yokalona.tree.TestHelper.*;
import static org.junit.jupiter.api.Assertions.*;

public class PersistentArrayTest {

    public static final int REPEATS = Power.two(5);
    public static final int MAX_CHUNK_SIZE = Power.two(19);
    private static final int[] size = {Power.two(5), Power.two(15), Power.two(20)};
    private static final float[] loadFactor = {.01F, .05F, .1F, .25F};

    private Path path;

    @BeforeEach
    public void
    setUp() throws IOException {
        this.path = Files.createTempDirectory("array");
    }

    @AfterEach
    public void
    tearDown() throws IOException {
        Files.list(path).map(Path::toFile).forEach(File::delete);
    }

//    @ParameterizedTest
//    @MethodSource("generate")
    public void
    testRabbitAndTheHat(PersistenceTest test) throws IOException {
        CountingSubscriber statist = new CountingSubscriber();
        int length = test.size();
        Configuration configuration = configure(
                file(path.resolve("rabbitAndTheHat.la"))
                        .mode(RW)
                        .buffer(test.buffer())
                        .cached())
                .memory(chunked(test.memory()))
                .addSubscriber(statist)
                .read(read().breakOnLoaded().chunked(test.read()))
                .write(write().chunked(test.write()));
        long writeLinear = 0L, readLinear = 0L, writeRandom = 0L, readRandom = 0L;
        printConfiguration(test);
        for (int i = 0; i < REPEATS; i++) {
            printHeader(i + 1, REPEATS);
            printStatistic();
            writeLinear += printStatistic("Linear write", writeLinear(length, configuration, statist), statist);
            readLinear += printStatistic("Linear read", readLinear(length, configuration, true), statist);
            writeRandom += printStatistic("Random write", writeRandom(length, configuration, statist), statist);
            readRandom += printStatistic("Random read", readRandom(length, configuration, true), statist);
        }
        printHeader();
        printAverage("linear write", (float) writeLinear / REPEATS, "ms");
        printAverage("linear read", (float) readLinear / REPEATS, "ms");
        printAverage("random write", (float) writeRandom / REPEATS, "ms");
        printAverage("random read", (float) readRandom / REPEATS, "ms");
        printAverage("read collisions", statist.average(Counter.CACHE_MISS, REPEATS), "ops");
        printAverage("write collisions", statist.average(Counter.WRITE_COLLISIONS, REPEATS), "ops");
        printStatistic("File size", getSize(Files.size(configuration.file().path())));
        printHeader();
    }

    @ParameterizedTest
    @ValueSource(floats = {.05F})
    public void
    testMemoryConsumption(float factor) throws IOException, InterruptedException {
        CountingSubscriber statist = new CountingSubscriber();
        int length = Power.two(30);
        Configuration configuration = configure(
                file(path.resolve(UUID.randomUUID() + ".la"))
                        .mode(RW)
                        .buffer(5 * Power.two(20))
                        .cached())
                .memory(chunked(Power.two(17)))
                .addSubscriber(statist)
                .read(read().breakOnLoaded().chunked(Power.two(17)))
                .write(write().chunked(Power.two(17)));
        long writeLinear = 0L, readLinear = 0L, writeRandom = 0L, readRandom = 0L;
        printConfiguration(new PersistenceTest(length, configuration.memory().size(),
                configuration.read().size(),
                configuration.write().size(),
                configuration.file().buffer(),
                CompactInteger.descriptor.size()));
        for (int i = 0; i < 1; i++) {
            printHeader(i + 1, 1);
            writeLinear += printStatistic("Linear write", writeLinear(length, configuration, statist), statist);
            readLinear += printStatistic("Linear read", readLinear(length, configuration, true), statist);
//            writeRandom += printStatistic("Random write", writeRandom(length, configuration), statist);
//            readRandom += printStatistic("Random read", readRandom(length, configuration, true), statist);
        }
        printHeader();
        printAverage("linear write", (float) writeLinear / 1, "ms");
        printAverage("linear read", (float) readLinear / 1, "ms");
        printAverage("random write", (float) writeRandom / 1, "ms");
        printAverage("random read", (float) readRandom / 1, "ms");
        printAverage("read collisions", statist.average(Counter.CACHE_MISS, 1), "ops");
        printAverage("write collisions", statist.average(Counter.WRITE_COLLISIONS, 1), "ops");
        printStatistic("File size", getSize(Files.size(configuration.file().path())));
        printHeader();
    }

    private static long
    readLinear(int length, Configuration configuration, boolean validate) {
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            long start = System.currentTimeMillis();
            for (int index = 0; index < length; index++) {
                CompactInteger value = array.get(index);
                if (validate) {
                    assertNotNull(value);
                    assertEquals(index, value.value());
                }
//                printProgress(length, index);
            }
//            System.err.print("\r");
//            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static long
    readRandom(int length, Configuration configuration, boolean validate) {
        int[] indices = new int[length];
        for (int index = 0; index < length; index++) {
            indices[index] = index;
        }
        shuffle(indices);
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            long start = System.currentTimeMillis();
            int ops = 0;
            for (int index : indices) {
                CompactInteger value = array.get(index);
                if (validate) {
                    assertNotNull(value);
                    assertEquals(index, value.value());
                }
                printProgress(length, ++ops);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static long
    writeLinear(int length, Configuration configuration, CountingSubscriber statist) {
        long fileStart = System.currentTimeMillis();
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            printStatistic("Creation time", (statist.get(Counter.FILE_CREATED) - fileStart) + " ms");
            long start = System.currentTimeMillis();
            for (int index = 0; index < length; index++) {
                array.set(index, new CompactInteger(index));
//                printProgress(length, index);
            }
//            System.out.print("\r");
//            System.out.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static long
    writeRandom(int length, Configuration configuration, CountingSubscriber statist) {
        int[] indices = new int[length];
        for (int index = 0; index < length; index++) {
            indices[index] = index;
        }
        shuffle(indices);
        long fileStart = System.currentTimeMillis();
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            printStatistic("Creation time", (statist.get(Counter.FILE_CREATED) - fileStart) + " ms");
            long start = System.currentTimeMillis();
            int ops = 0;
            for (int index : indices) {
                array.set(index, new CompactInteger(index));
                printProgress(length, ++ops);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static void printProgress(int length, int index) {
        int del = Math.max(1, length / 100);
        if (index % del == 0) {
            System.out.printf("\r|-record %10d out of %-10d-----+--------------------|", index, length);
            System.out.flush();
        }
    }

    private static long
    readLargeRandom(int length, Configuration configuration) {
        Random random = new Random();
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            long start = System.currentTimeMillis();
            int ops = 0;
            for (int index = 0; index < length; index++) {
                int value = random.nextInt(length);
                CompactInteger result = array.get(value);
                if (result != null) {
                    assertEquals(value, result.value());
                }
                printProgress(length, ++ops);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    private static long
    writeLargeRandom(int length, Configuration configuration) {
        Random random = new Random();
        try (var array = new PersistentArray<>(length, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            long start = System.currentTimeMillis();
            int ops = 0;
            for (int index = 0; index < length; index++) {
                int value = random.nextInt(length);
                array.set(value, new CompactInteger(value));
                printProgress(length, ++ops);
            }
            System.err.print("\r");
            System.err.flush();
            return System.currentTimeMillis() - start;
        }
    }

    public static List<PersistenceTest>
    generate() {
        List<PersistenceTest> generated = new ArrayList<>();
        for (int size : size)
            for (float factor : loadFactor) {
                int chunkSize = Math.min(MAX_CHUNK_SIZE, Math.max((int) (size * factor), 1));
                generated.add(new PersistenceTest(size, chunkSize, chunkSize, chunkSize, Power.two(10), CompactInteger.descriptor.size()));
            }
        return generated;
    }
}

/*

|--------------------------------------------------------------|
|     Average linear write                |       2.13 ms      |
|     Average linear read                 |       0.69 ms      |
|     Average random write                |       5.06 ms      |
|     Average random read                 |       8.13 ms      |
|     Average collisions                  |     116.88 ops     |
|--------------------------------------------------------------|
|     Average linear write                |       0.38 ms      |
|     Average linear read                 |       0.44 ms      |
|     Average random write                |       4.66 ms      |
|     Average random read                 |       6.56 ms      |
|     Average collisions                  |      78.84 ops     |
|--------------------------------------------------------------|
|     Average linear write                |       1.19 ms      |
|     Average linear read                 |       0.50 ms      |
|     Average random write                |       4.50 ms      |
|     Average random read                 |       9.72 ms      |
|     Average collisions                  |      43.66 ops     |
|--------------------------------------------------------------|
 */