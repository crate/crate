package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-update-planner")
public class QueryPlannerUpdateBenchmark extends UpdateBenchmark {

    @Override
    public boolean isQueryPlannerEnabled() {
        return true;
    }
}
