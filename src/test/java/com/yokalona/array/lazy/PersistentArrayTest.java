package com.yokalona.array.lazy;

import com.yokalona.array.persitent.*;
import com.yokalona.array.persitent.configuration.Configuration;
import com.yokalona.array.persitent.debug.CompactInteger;
import com.yokalona.array.persitent.exceptions.FileMarkedForDeletingException;
import com.yokalona.array.persitent.exceptions.HeaderMismatchException;
import com.yokalona.array.persitent.exceptions.IncompatibleVersionException;
import com.yokalona.array.persitent.io.FixedObjectLayout;
import com.yokalona.array.persitent.serializers.Serializers;
import com.yokalona.array.persitent.subscriber.CountingSubscriber;
import com.yokalona.array.persitent.subscriber.CountingSubscriber.Counter;
import com.yokalona.array.persitent.util.Power;
import com.yokalona.tree.TestHelper.PersistenceTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.yokalona.array.persitent.configuration.Chunked.chunked;
import static com.yokalona.array.persitent.configuration.ChunkedRead.read;
import static com.yokalona.array.persitent.configuration.ChunkedWrite.write;
import static com.yokalona.array.persitent.configuration.File.Mode.*;
import static com.yokalona.array.persitent.configuration.File.file;
import static com.yokalona.array.persitent.configuration.Configuration.configure;
import static com.yokalona.tree.TestHelper.*;
import static org.junit.jupiter.api.Assertions.*;

public class PersistentArrayTest {

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

    @Test
    public void
    testCreateArrayCreatesFileOnDisk() throws IOException {
        Path filePath = path.resolve("testCreateArrayCreatesFileOnDisk.la");
        try(var ignore = new PersistentArray<>(10, CompactInteger.descriptor, FixedObjectLayout::new,
                configure(file(filePath).cached())
                        .memory(chunked(10))
                        .read(read().chunked(10))
                        .write(write().chunked(10)))) {
            assertTrue(Files.exists(filePath));
            assertTrue(0 < Files.size(filePath));
        }
    }

    @Test
    public void
    testValidateHeader() throws IOException {
        Path filePath = path.resolve("testValidateHeader.la");
        Configuration configuration = configure(file(filePath).cached())
                .memory(chunked(10))
                .read(read().chunked(10))
                .write(write().chunked(10));
        try(var ignore = new PersistentArray<>(10, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            assertTrue(Files.exists(filePath));
        }
        try (var file = new RandomAccessFile(filePath.toFile(), "rw")) {
            file.seek(1);
            file.write(0);
        }
        assertThrows(HeaderMismatchException.class, () -> PersistentArray.deserialize(CompactInteger.descriptor, configuration));
    }

    @Test
    public void
    testValidateVersion() throws IOException {
        Path filePath = path.resolve("testValidateVersion.la");
        Configuration configuration = configure(file(filePath).cached())
                .memory(chunked(10))
                .read(read().chunked(10))
                .write(write().chunked(10));
        try(var ignore = new PersistentArray<>(10, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            assertTrue(Files.exists(filePath));
        }
        try (var file = new RandomAccessFile(filePath.toFile(), "rw")) {
            file.seek(7);
            file.write(5);
        }
        assertThrows(IncompatibleVersionException.class, () -> PersistentArray.deserialize(CompactInteger.descriptor, configuration));
    }

    @Test
    public void
    testFileMarkedForRemoval() throws IOException {
        Path filePath = path.resolve("testFileMarkedForRemoval.la");
        Configuration configuration = configure(file(filePath).cached())
                .memory(chunked(10))
                .read(read().chunked(10))
                .write(write().chunked(10));
        try(var ignore = new PersistentArray<>(10, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            assertTrue(Files.exists(filePath));
        }
        try (var file = new RandomAccessFile(filePath.toFile(), "rw")) {
            file.seek(10);
            file.write(1);
        }
        assertThrows(FileMarkedForDeletingException.class, () -> PersistentArray.deserialize(CompactInteger.descriptor, configuration));
    }

    @Test
    public void
    testDataIsWritten() throws IOException {
        Path filePath = path.resolve("testDataIsWritten.la");
        Configuration configuration = configure(file(filePath).cached())
                .memory(chunked(10))
                .read(read().chunked(10))
                .write(write().chunked(10));
        try(var array = new PersistentArray<>(10, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            assertTrue(Files.exists(filePath));
            for (int i = 0; i < array.length(); i ++) {
                array.set(i, new CompactInteger(i + 10));
            }
        }
        try (var file = new RandomAccessFile(filePath.toFile(), "rw")) {
            file.seek(11);
            byte[] bytes = new byte[5];
            file.read(bytes);
            assertEquals(10, Serializers.deserialize(bytes, 0));
        }
    }

    @Test
    public void
    testDataIsWrittenAndCanBeReadLater() throws IOException {
        Path filePath = path.resolve("testDataIsWrittenAndCanBeReadLater.la");
        Configuration configuration = configure(file(filePath).cached())
                .memory(chunked(10))
                .read(read().chunked(10))
                .write(write().chunked(10));
        try(var array = new PersistentArray<>(10, CompactInteger.descriptor, FixedObjectLayout::new, configuration)) {
            assertTrue(Files.exists(filePath));
            for (int i = 0; i < array.length(); i ++) {
                array.set(i, new CompactInteger(i + 10));
            }
        }
        try (var array = PersistentArray.deserialize(CompactInteger.descriptor, configuration)) {
            for (int i = 0; i < array.length(); i ++) {
                assertEquals(i + 10, array.get(i).value());
            }
        }
    }

//    @ParameterizedTest
//    @ValueSource(floats = {.05F})
    public void
    testMemoryConsumption(float factor) throws IOException, InterruptedException {
        CountingSubscriber statist = new CountingSubscriber();
        int length = Power.two(10);
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
            readLinear += printStatistic("Linear read", readLinear(length, configuration, false), statist);
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
                printProgress(length, index);
            }
            System.err.print("\r");
            System.err.flush();
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
                printProgress(length, index);
            }
            System.out.print("\r");
            System.out.flush();
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

}