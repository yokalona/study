package com.yokalona.tree.b;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import static com.yokalona.tree.b.Helper.formSample;
import static com.yokalona.tree.b.Helper.randomKey;

@State(Scope.Benchmark)
public class BTreeGetPerformanceTest {

    @State(Scope.Benchmark)
    public static class ExecutionPlan {
        @Param({"4"/*, "32", "256", "2048", "16384", "131072", "262144"*/})
        public int capacity;

        @Param({"10", "100"/*, "1000", "10000", "100000", "1000000"*/})
        public int sampleSize;

        private Helper.SpyKey[] data;

        public BTree<Helper.SpyKey, Integer> bTree;

        @Setup(Level.Trial)
        public void prepareData() {
            this.data = formSample(sampleSize);
            this.bTree = new BTree<>(capacity);
            for (Helper.SpyKey key : data) {
                bTree.insert(key, key.key());
            }
        }

    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void btree_get(ExecutionPlan executionPlan, Blackhole blackhole) {
        Helper.SpyKey.reset();
        Helper.SpyKey[] data = executionPlan.data;
        BTree<Helper.SpyKey, Integer> bTree = executionPlan.bTree;
        Helper.SpyKey key = randomKey(data);
        blackhole.consume(bTree.get(key));
    }

}