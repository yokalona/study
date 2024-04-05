package com.yokalona.tree.b;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class BTreePerformanceTest {
    int sampleSize = 1_000_000;

    private final Integer[] data = formSample(sampleSize);

    @BeforeEach
    public void before() {
        shuffle(data);
    }

    @Order(0)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void simpleInsertTest(int capacity) {
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        try (var ignore = new StopWatch(capacity, "Simple insert test on B-Tree")) {
            for (int sample : data) bTree.insert(sample, sample);
        }
    }

    @Order(10)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void repeatedInsertTest(int capacity) {
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        try (var ignore = new StopWatch(capacity, "Repeated insert test on B-Tree")) {
            for (int sample : data) bTree.insert(sample, sample);
            shuffle(data);
            for (int ignored : data) bTree.insert(data[0], data[0]);
        }
    }

    @Order(20)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void ladderInsertTest(int capacity) {
        int iterations = 100;
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        try (var ignore = new StopWatch(capacity, "Ladder insert test on B-Tree")) {
            for (int iteration = 0; iteration < iterations; iteration++) {
                for (int sample = 0; sample < data.length / iterations; sample++) {
                    Integer datum = data[sample + (data.length / iterations) * iteration];
                    bTree.insert(datum, datum);
                }
                shuffle(data);
            }
        }
    }

    @Order(30)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void simpleRemoveTest(int capacity) {
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        for (int sample : data) bTree.insert(sample, sample);
        shuffle(data);
        try (var ignore = new StopWatch(capacity, "Simple remove test on B-Tree")) {
            for (int sample : data) bTree.remove(sample);
        }
    }

    @Order(40)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void repeatedRemoveTest(int capacity) {
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        for (int sample : data) bTree.insert(sample, sample);
        try (var ignore = new StopWatch(capacity, "Repeated remove test on B-Tree")) {
            for (int ignored : data) bTree.insert(data[0], data[0]);
        }
    }

    @Order(50)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void ladderRemoveTest(int capacity) {
        int iterations = 100;
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        for (int sample : data) bTree.insert(sample, sample);
        shuffle(data);
        try (var ignore = new StopWatch(capacity, "Ladder remove test on B-Tree")) {
            for (int iteration = 0; iteration < iterations; iteration++) {
                for (int sample = 0; sample < data.length / iterations; sample++) {
                    Integer datum = data[sample + (data.length / iterations) * iteration];
                    bTree.remove(datum);
                }
                shuffle(data);
            }
        }
    }

    @Order(60)
    @ParameterizedTest
    @MethodSource("loadParameters")
    public void ladderInsertRemoveTest(int capacity) {
        int iterations = 100;
        BTree<Integer, Integer> bTree = new BTree<>(capacity);
        try (var ignore = new StopWatch(capacity, "Ladder insert-remove test on B-Tree")) {
            for (int iteration = 0; iteration < iterations; iteration++) {
                for (int sample = 0; sample < data.length / iterations; sample++) {
                    Integer datum = data[sample + (data.length / iterations) * iteration];
                    bTree.insert(datum, datum);
                }
                shuffle(data);
                for (int sample = 0; sample < data.length / iterations; sample++) {
                    Integer datum = data[sample + (data.length / iterations) * iteration];
                    bTree.remove(datum);
                }
            }
        }
    }


    @State(Scope.Benchmark)
    public static class ExecutionPlan {
        @Param({"4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048"})
        public int capacity;

        @Param({"1000", "10000", "100000", "1000000"})
        public int sampleSize;
        private Integer[] data;

        public BTree<Integer, Integer> bTree;

        @Setup(Level.Invocation)
        public void setUp() {
            data = formSample(sampleSize);
            bTree = new BTree<>(capacity);
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void benchmark(ExecutionPlan executionPlan, Blackhole blackhole) {
        Integer[] data = executionPlan.data;
        BTree<Integer, Integer> bTree = executionPlan.bTree;
        for (int sample : data) {
            bTree.insert(sample, sample);
        }
        blackhole.consume(bTree);
    }

    public static void main(String[] args) throws IOException {
        org.openjdk.jmh.Main.main(args);
    }

    private static class StopWatch implements Closeable {
        private final long start;
        private final int capacity;
        private final String message;

        private StopWatch(int capacity, String message) {
            this.message = message;
            this.capacity = capacity;
            start = System.currentTimeMillis();
        }

        @Override
        public void close() {
            long end = System.currentTimeMillis();
            System.out.printf(message + " with capacity: %5d took %10s ms%n", capacity,
                    Duration.of(end - start, TimeUnit.MILLISECONDS.toChronoUnit()).toMillis());
        }

    }

    private static int[] loadParameters() {
        return new int[]{4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};
    }

    private static Integer[] formSample(int sampleSize) {
        Integer[] data = new Integer[sampleSize];
        for (int testSize = 0; testSize < sampleSize; testSize++)
            data[testSize] = testSize;
        shuffle(data);
        return data;
    }

    public static <Type extends Comparable<Type>> void shuffle(Type[] array) {
        for (int idx = array.length - 1; idx > 0; idx--) {
            int element = (int) Math.floor(Math.random() * (idx + 1));
            swap(array, idx, element);
        }
    }

    private static <Type extends Comparable<Type>> void swap(Type[] arr, int left, int right) {
        Type tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
    }

}