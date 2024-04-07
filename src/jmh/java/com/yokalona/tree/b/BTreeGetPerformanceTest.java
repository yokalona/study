package com.yokalona.tree.b;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class BTreeGetPerformanceTest {

    public static final Random RANDOM = new Random();

    @State(Scope.Benchmark)
    public static class GetExecutionPlan {
        @Param({"4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "16384"})
        public int capacity;

        @Param({"1000", "10000", "100000", "1000000"})
        public int sampleSize;

        private Integer[] data;

        public BTree<Integer, Integer> bTree;

        @Setup(Level.Trial)
        public void prepareData() {
            this.data = formSample(sampleSize);
        }

        @Setup(Level.Invocation)
        public void fillTree() {
            this.bTree = new BTree<>(capacity);
            for (int key : data) {
                bTree.insert(key, key);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class ControllExecutionPlan {
        @Param({"1000", "10000", "100000", "1000000"})
        public int sampleSize;

        private Integer[] data;

        public HashMap<Integer, Integer> hashMap;
        public TreeMap<Integer, Integer> treeMap;

        @Setup(Level.Trial)
        public void prepareData() {
            this.data = formSample(sampleSize);
        }

        @Setup(Level.Invocation)
        public void fillTree() {
            this.hashMap = new HashMap<>();
            this.treeMap = new TreeMap<>();
            for (int key : data) {
                hashMap.put(key, key);
                treeMap.put(key, key);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void btree(GetExecutionPlan executionPlan, Blackhole blackhole) {
        Integer[] data = executionPlan.data;
        BTree<Integer, Integer> bTree = executionPlan.bTree;
        Integer key = randomKey(data);
        Integer value = bTree.get(key);
        if (!key.equals(value)) throw new IllegalStateException();
        blackhole.consume(value);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void hashmap(ControllExecutionPlan executionPlan, Blackhole blackhole) {
        Integer[] data = executionPlan.data;
        HashMap<Integer, Integer> hashMap = executionPlan.hashMap;
        blackhole.consume(hashMap.get(randomKey(data)));
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void treemap(ControllExecutionPlan executionPlan, Blackhole blackhole) {
        Integer[] data = executionPlan.data;
        TreeMap<Integer, Integer> treeMap = executionPlan.treeMap;
        blackhole.consume(treeMap.get(randomKey(data)));
    }

    Integer randomKey(Integer[] data) {
        return data[RANDOM.nextInt(data.length)];
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(BTreeGetPerformanceTest.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .resultFormat(ResultFormatType.JSON)
                .forks(1)
                .build();

        new Runner(options).run();
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