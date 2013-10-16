package com.carrotsearch.junitbenchmarks;

import java.util.List;

/**
 * Calculate simple statistics from multiple {@link com.carrotsearch.junitbenchmarks.SingleResult}s.
 */
final class Statistics
{
    public Average gc;
    public Average evaluation;
    public Average blocked;

    public static Statistics from(List<SingleResult> results)
    {
        final Statistics stats = new Statistics();
        long [] times = new long [results.size()];

        // GC-times.
        for (int i = 0; i < times.length; i++)
            times[i] = results.get(i).gcTime();
        stats.gc = Average.from(times);

        // Evaluation-only times.
        for (int i = 0; i < times.length; i++)
            times[i] = results.get(i).evaluationTime();
        stats.evaluation = Average.from(times);

        // Thread blocked times.
        for (int i = 0; i < times.length; i++)
            times[i] = results.get(i).blockTime;
        stats.blocked = Average.from(times);


        return stats;
    }
}