package com.carrotsearch.junitbenchmarks;

import org.junit.Rule;
import org.junit.rules.TestRule;

/**
 * A superclass for tests that should be executed as benchmarks (several rounds, GC and
 * time accounting). Provides a JUnit rule in {@link #benchmarkRun} that runs the tests
 * repeatedly, logging the intermediate results (memory usage, times).
 * <p>
 * Subclasses may add {@link com.carrotsearch.junitbenchmarks.BenchmarkOptions} at the class-level or to individual methods
 * to override the defaults.
 * </p>
 */
public abstract class AbstractBenchmark
{
    /**
     * Enables the benchmark rule.
     */
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();
}
