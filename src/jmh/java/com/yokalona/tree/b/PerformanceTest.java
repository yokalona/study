package com.yokalona.tree.b;

import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PerformanceTest {

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(5)
                .resultFormat(ResultFormatType.JSON)
                .result("benchmarks/output.json")
                .addProfiler(JavaFlightRecorderProfiler.class, "dir=jfr")
                .include(BTreeGetPerformanceTest.class.getSimpleName())
                .include(BTreeInsertPerformanceTest.class.getSimpleName())
                .build();

        new Runner(options).run();
    }
}
