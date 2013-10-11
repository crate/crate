package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix="benchmark-delete-planner")
public class QueryPlannerDeleteBenchmark extends DeleteBenchmark {

    @Override
    public boolean isQueryPlannerEnabled() {
        return true;
    }
}
