package com.carrotsearch.junitbenchmarks;

import java.util.Locale;

public final class Average {
    /**
     * Average (in milliseconds).
     */
    public final double avg;

    /**
     * Standard deviation (in milliseconds).
     */
    public final double stddev;

    /**
     *
     */
    Average(double avg, double stddev)
    {
        this.avg = avg;
        this.stddev = stddev;
    }

    public String toString()
    {

        return String.format(Locale.ENGLISH, "%.6g [+- %.6g]",
                avg, stddev);
    }

    static Average from(long [] values)
    {
        long sum = 0;
        long sumSquares = 0;

        for (long l : values)
        {
            sum += l;
            sumSquares += l * l;
        }

        double avg = sum / (double) values.length;
        return new Average(
                (sum / (double) values.length) / 1000.0,
                Math.sqrt(sumSquares / (double) values.length - avg * avg) / 1000.0);
    }
}
