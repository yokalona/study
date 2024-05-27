package com.yokalona.tree.b.array;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class GetTest {

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void
    sequentialGet(GetExecutionPlan executionPlan, Blackhole blackhole) {
        int next = executionPlan.nextLinear();
        blackhole.consume(executionPlan.array.get(next));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void
    randomGet(GetExecutionPlan executionPlan, Blackhole blackhole) {
        int next = executionPlan.nextRandom();
        blackhole.consume(executionPlan.array.get(next));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void
    highCollision(GetExecutionPlan executionPlan, Blackhole blackhole) {
        int next = executionPlan.nextCollisional();
        blackhole.consume(executionPlan.array.get(next));
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .result("benchmarks/array-get-output.json")
                .include(GetTest.class.getSimpleName())
                .build();

        new Runner(options).run();
    }

}