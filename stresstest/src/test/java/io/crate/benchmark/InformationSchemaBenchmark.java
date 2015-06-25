package io.crate.benchmark;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BenchmarkHistoryChart(filePrefix="benchmark-information-scheme-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-information-scheme")
public class InformationSchemaBenchmark extends BenchmarkBase {

    public static final int NUM_REQUESTS_PER_TEST = 10;
    public static final int BENCHMARK_ROUNDS = 100;
    public static final int WARMUP_ROUNDS = 10;

    private boolean dataCreated = false;

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    public boolean importData() {
        return false;
    }

    @Before
    public void prepare() throws Exception {
        if (!dataCreated) {
            for (int i=0; i < 50; i++) {
                String stmt = String.format("create table %s.t (name string) with (number_of_replicas=0)", randomAsciiOfLength(5));
                execute(stmt);
            }
            dataCreated = true;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testSelect() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            execute("select schema_name from information_schema.tables where table_name = 't'");
        }
    }

}
