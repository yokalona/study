package com.yokalona.tree.b.array;

import com.yokalona.array.persitent.debug.CompactInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class SetTest {

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void
    sequentialSet(SetExecutionPlan executionPlan) {
        int next = executionPlan.nextLinear();
        executionPlan.array.set(next, new CompactInteger(next));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void
    randomSet(SetExecutionPlan executionPlan) {
        int next = executionPlan.nextRandom();
        executionPlan.array.set(next, new CompactInteger(next));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void
    highCollision(SetExecutionPlan executionPlan) {
        int next = executionPlan.nextCollisional();
        executionPlan.array.set(next, new CompactInteger(next));
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .result("benchmarks/array-set-output.json")
                .include(SetTest.class.getSimpleName())
                .build();

        new Runner(options).run();
    }

}