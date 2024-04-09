package com.yokalona.tree.b;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static com.yokalona.tree.b.Helper.*;

@State(Scope.Benchmark)
public class BTreeInsertPerformanceTest {

    public static final int OPERATIONS = 20000;

    @State(Scope.Benchmark)
    public static class ExecutionPlan {
        @Param({"4", "32", "256", "2048", "16384", "131072"})
        public int capacity;
        public int key = 0;
        public Integer[] data;

        public BTree<Integer, Integer> bTree;

        @Setup(Level.Trial)
        public void fill() {
            this.data = new Integer[OPERATIONS];
            for (int i = 0; i < OPERATIONS; i++) {
                data[i] = i;
            }
        }

        @Setup(Level.Iteration)
        public void prepareData() {
            this.bTree = new BTree<>(capacity);
            shuffle(data);
            key = 0;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 100, batchSize = OPERATIONS)
    @Measurement(iterations = 50, batchSize = OPERATIONS)
    @OperationsPerInvocation(OPERATIONS)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void btree_insert_base(ExecutionPlan executionPlan, Blackhole blackhole) {
        BTree<Integer, Integer> bTree = executionPlan.bTree;
        Integer[] data = executionPlan.data;
        blackhole.consume(bTree.insert(data[executionPlan.key++], executionPlan.key));
    }

}