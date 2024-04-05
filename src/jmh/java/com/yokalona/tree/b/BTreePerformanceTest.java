package com.yokalona.tree.b;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class BTreePerformanceTest {

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