/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.statistics;

import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.types.DataType;
import io.crate.types.FixedWidthType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

public final class ColumnStats implements Writeable {

    static final int MCV_TARGET = 100;

    private final double nullFraction;
    private final double averageSizeInBytes;
    private final double approxDistinct;

    public ColumnStats(double nullFraction, double averageSizeInBytes, double approxDistinct) {
        this.nullFraction = nullFraction;
        this.averageSizeInBytes = averageSizeInBytes;
        this.approxDistinct = approxDistinct;
    }

    public ColumnStats(StreamInput in) throws IOException {
        this.nullFraction = in.readDouble();
        this.averageSizeInBytes = in.readDouble();
        this.approxDistinct = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(nullFraction);
        out.writeDouble(averageSizeInBytes);
        out.writeDouble(approxDistinct);
    }

    public double averageSizeInBytes() {
        return averageSizeInBytes;
    }

    public double nullFraction() {
        return nullFraction;
    }

    public double approxDistinct() {
        return approxDistinct;
    }

    /**
     * @param samples sorted sample values (must exclude any null values that might have been sampled)
     * @param nullCount Number of null values that have been excluded from the samples
     */
    public static <T> ColumnStats fromSortedValues(List<T> samples,
                                                   DataType<T> type,
                                                   int nullCount,
                                                   long numTotalRows) {
        // This is heavily inspired by the logic in PostgreSQL (src/backend/command/analyze.c -> compute_scalar_stats)

        int notNullCount = samples.size();
        if (notNullCount == 0 && nullCount > 0) {
            // Only null values found, assume all values are null
            double nullFraction = 1.0;
            int averageWidth = type instanceof FixedWidthType ? ((FixedWidthType) type).fixedSize() : 0;
            long approxDistinct = 1;
            return new ColumnStats(nullFraction, averageWidth, approxDistinct);
        }
        boolean isVariableLength = !(type instanceof FixedWidthType);
        SizeEstimator<T> sizeEstimator = SizeEstimatorFactory.create(type);
        int mcvTarget = Math.min(MCV_TARGET, samples.size());
        MVCItem[] mostCommonValueCandidates = initMCVItems(mcvTarget);
        long totalSampleValueSizeInBytes = 0;
        int numTracked = 0;
        int duplicates = 0;
        int distinctValues = 0;
        int numValuesWithDuplicates = 0;
        for (int i = 0; i < samples.size(); i++) {
            T currentValue = samples.get(i);
            if (isVariableLength) {
                totalSampleValueSizeInBytes += sizeEstimator.estimateSize(currentValue);
            }
            if (i == 0) {
                continue;
            }
            T previousValue = samples.get(i - 1);
            duplicates++;
            boolean sameAsPrevious = currentValue.equals(previousValue);
            boolean lastSample = i == samples.size() - 1;
            if (sameAsPrevious && !lastSample) {
                continue;
            }
            distinctValues++;
            if (duplicates > 1) {
                numValuesWithDuplicates++;
                if (numTracked < mcvTarget || duplicates > mostCommonValueCandidates[numTracked - 1].count) {
                    if (numTracked < mcvTarget) {
                        numTracked++;
                    }
                    updateMostCommonValues(mostCommonValueCandidates, numTracked, duplicates, i);
                }
            }
            duplicates = 0;
        }
        double nullFraction = (double) nullCount / (double) samples.size();
        double averageSizeInBytes = isVariableLength
            ? totalSampleValueSizeInBytes / (double) notNullCount
            : ((FixedWidthType) type).fixedSize();
        double approxDistinct = approximateNumDistinct(
            nullFraction,
            distinctValues,
            numValuesWithDuplicates,
            samples.size(),
            numTotalRows
        );
        // TODO: Decide which most-common-values to keep and keep them
        // TODO: Generate histogram
        return new ColumnStats(nullFraction, averageSizeInBytes, approxDistinct);
    }

    private static double approximateNumDistinct(double nullFraction,
                                                 int distinctSamples,
                                                 int numValuesWithDuplicates,
                                                 int samplesWithoutNulls,
                                                 long totalRows) {
        if (numValuesWithDuplicates == 0) {
            // All sample values are unique; We assume it is a unique column, but we've to discount for null values:
            return (1.0 - nullFraction) * totalRows;
        }
        if (numValuesWithDuplicates == distinctSamples) {
            // Every value appeared more than once. Assume the column has just these columns.
            // This should address cases like boolean or enum columns, where there is a small fixed set of possible values.
            return distinctSamples;
        }
        /*
         * Estimate the number of distinct values using the estimator
         * proposed by Haas and Stokes in IBM Research Report RJ 10025:
         *    n*d / (n - f1 + f1*n/N)
         * where f1 is the number of distinct values that occurred
         * exactly once in our sample of n rows (from a total of N),
         * and d is the total number of distinct values in the sample.
         * This is their Duj1 estimator; the other estimators they
         * recommend are considerably more complex, and are numerically
         * very unstable when n is much smaller than N.
         */
        int f1 = distinctSamples - numValuesWithDuplicates;
        int d = f1 + numValuesWithDuplicates;
        @SuppressWarnings("UnnecessaryLocalVariable")
        double n = samplesWithoutNulls;
        double N = totalRows * (1.0 - nullFraction);
        double approxDistinct;
        if (N > 0) {
            approxDistinct = (n * d) / ((n - f1) + f1 * n / N);
        } else {
            approxDistinct = 0;
        }
        /* Clamp to sane range in case of round-off error */
        if (approxDistinct < d) {
            approxDistinct = d;
        }
        if (approxDistinct > N) {
            approxDistinct = N;
        }
        return Math.floor(approxDistinct + 0.5);
    }

    private static void updateMostCommonValues(MVCItem[] mostCommonValueCandidates,
                                               int numTracked,
                                               int duplicates,
                                               int i) {
        // If the new candidate has more duplicates than previously tracked MCVs we insert it and bubble others down
        int j;
        for (j = numTracked - 1; j > 0; j--) {
            if (duplicates <= mostCommonValueCandidates[j - 1].count) {
                continue;
            }
            mostCommonValueCandidates[j].count = mostCommonValueCandidates[j - 1].count;
            mostCommonValueCandidates[j].first = mostCommonValueCandidates[j - 1].first;
        }
        mostCommonValueCandidates[j].count = duplicates;
        mostCommonValueCandidates[j].first = i - duplicates;
    }

    private static MVCItem[] initMCVItems(int mcvTarget) {
        MVCItem[] trackedValues = new MVCItem[mcvTarget];
        for (int i = 0; i < trackedValues.length; i++) {
            trackedValues[i] = new MVCItem();
        }
        return trackedValues;
    }

    static class MVCItem {

        int first = 0;
        int count = 0;

        @Override
        public String toString() {
            return "MVCItem{" +
                   "first=" + first +
                   ", count=" + count +
                   '}';
        }
    }
}
