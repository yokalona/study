package com.yokalona.tree.b;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import static com.yokalona.tree.b.Helper.formSample;
import static com.yokalona.tree.b.Helper.randomKey;

@State(Scope.Benchmark)
public class BTreeRemovePerformanceTest {
    public static final int OPERATIONS = 1000;

    @State(Scope.Benchmark)
    public static class ExecutionPlan {
        @Param({"4", "32", "256", "2048", "16384", "131072"})
        public int capacity;
        public int key = 0;

        public BTree<Integer, Integer> bTree;

        @Setup(Level.Iteration)
        public void prepareData() {
            Integer[] data = formSample(1000000);
            this.bTree = new BTree<>(capacity);
            for (int key : data) {
                bTree.insert(key, key);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 50, batchSize = OPERATIONS)
    @Measurement(iterations = 50, batchSize = OPERATIONS)
    @OperationsPerInvocation(OPERATIONS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void btree_remove_base(ExecutionPlan executionPlan, Blackhole blackhole) {
        BTree<Integer, Integer> bTree = executionPlan.bTree;
        blackhole.consume(bTree.remove(executionPlan.key ++));
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 50, batchSize = OPERATIONS * 10)
    @Measurement(iterations = 50, batchSize = OPERATIONS * 10)
    @OperationsPerInvocation(OPERATIONS * 10)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void btree_remove_base_10(ExecutionPlan executionPlan, Blackhole blackhole) {
        BTree<Integer, Integer> bTree = executionPlan.bTree;
        blackhole.consume(bTree.remove(executionPlan.key ++));
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 50, batchSize = OPERATIONS * 100)
    @Measurement(iterations = 50, batchSize = OPERATIONS * 100)
    @OperationsPerInvocation(OPERATIONS * 100)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void btree_remove_base_100(ExecutionPlan executionPlan, Blackhole blackhole) {
        BTree<Integer, Integer> bTree = executionPlan.bTree;
        blackhole.consume(bTree.remove(executionPlan.key ++));
    }
}