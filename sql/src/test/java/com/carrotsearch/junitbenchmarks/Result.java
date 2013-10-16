package com.carrotsearch.junitbenchmarks;

import org.junit.runner.Description;

import java.lang.reflect.Method;
import java.util.logging.Logger;

/**
 * A result of a single benchmark test.
 */
public final class Result
{
    final Description description;

    public final int benchmarkRounds, warmupRounds;
    public final long warmupTime, benchmarkTime;
    public final Average roundAverage;
    public final Average blockedAverage;
    public final Average gcAverage;
    public final GCSnapshot gcInfo;

    /**
     * Concurrency level (number of used threads).
     */
    int concurrency;

    /**
     * @param description Target object and method of the test.
     * @param benchmarkRounds Number of executed benchmark rounds.
     * @param warmupRounds Number of warmup rounds.
     * @param warmupTime Total warmup time, includes benchmarking and GC overhead.
     * @param benchmarkTime Total benchmark time, includes benchmarking and GC overhead.
     * @param roundAverage Average and standard deviation from benchmark rounds.
     * @param gcAverage Average and standard deviation from GC cleanups.
     * @param blockedAverage Average and standard deviation from thread blocks.
     * @param gcInfo Extra information about GC activity.
     * @param concurrency {@link com.carrotsearch.junitbenchmarks.BenchmarkOptions#concurrency()} setting (or global override).
     */
    public Result(
        Description description,
        int benchmarkRounds,
        int warmupRounds, 
        long warmupTime, 
        long benchmarkTime,
        Average roundAverage,
        Average blockedAverage,
        Average gcAverage,
        GCSnapshot gcInfo,
        int concurrency)
    {
        this.description = description;
        this.benchmarkRounds = benchmarkRounds;
        this.warmupRounds = warmupRounds;
        this.warmupTime = warmupTime;
        this.benchmarkTime = benchmarkTime;
        this.roundAverage = roundAverage;
        this.blockedAverage = blockedAverage;
        this.gcAverage = gcAverage;
        this.gcInfo = gcInfo;
        this.concurrency = concurrency;
    }

    /**
     * Returns the short version of the test's class.
     */
    public String getShortTestClassName()
    {
        return getTestClass().getSimpleName();
    }

    /**
     * Returns the long version of the test's class.
     */
    public String getTestClassName()
    {
        return this.description.getClassName();
    }

    /**
     * @return Return the test method's name.
     */
    public String getTestMethodName()
    {
        return this.description.getMethodName();
    }

    /**
     * Returns the class under test.
     */
    public Class<?> getTestClass()
    {
        return this.description.getTestClass();
    }

    /**
     * Returns the method under test (or null if not available and couldn't be located).
     */
    public Method getTestMethod() {
        try {
            return getTestClass().getMethod(getTestMethodName());
        } catch (NoSuchMethodException e) {
            Logger.getAnonymousLogger().warning(
                "Could not locate the test method responsible for test: "
                + getTestMethodName());
            return null;
        }
    }

    public int getThreadCount()
    {
        return concurrency;
    }
}
