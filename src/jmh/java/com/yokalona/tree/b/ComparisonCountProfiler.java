package com.yokalona.tree.b;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Aggregator;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ResultRole;
import org.openjdk.jmh.util.SingletonStatistics;
import org.openjdk.jmh.util.Statistics;

import java.util.Collection;
import java.util.List;

public class ComparisonCountProfiler implements InternalProfiler {

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        Helper.SpyKey.reset();
    }

    @Override
    public Collection<? extends Result<ComparisonCountResult>>
    afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result) {
        return List.of(new ComparisonCountResult(iterationParams.getCount(), new SingletonStatistics(Helper.SpyKey.count())));
    }

    @Override
    public String
    getDescription() {
        return "Counts the amount of 'compareOf' invocations on a Key";
    }

    public static class ComparisonCountResult extends Result<ComparisonCountResult> {

        private final int count;

        public ComparisonCountResult(int count, Statistics s) {
            super(ResultRole.SECONDARY, "Comparisons", s, "inv/it", AggregationPolicy.AVG);
            this.count = count;
        }

        @Override
        protected Aggregator<ComparisonCountResult>
        getThreadAggregator() {
            return this::aggregate;
        }

        @Override
        protected Aggregator<ComparisonCountResult>
        getIterationAggregator() {
            return this::aggregate;
        }

        private ComparisonCountResult aggregate(Collection<ComparisonCountResult> results) {
            double aggregatedScore = 0.0D;
            for (ComparisonCountResult result : results) {
                aggregatedScore += result.getScore();
            }
            return new ComparisonCountResult(1, new SingletonStatistics(aggregatedScore / count));
        }
    }
}
