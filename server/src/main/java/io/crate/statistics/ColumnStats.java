/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.statistics;

import io.crate.Streamer;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class ColumnStats<T> implements Writeable {

    private final double nullFraction;
    private final double averageSizeInBytes;
    private final double approxDistinct;
    private final DataType<T> type;
    private final MostCommonValues mostCommonValues;
    private final List<T> histogram;

    public ColumnStats(double nullFraction,
                       double averageSizeInBytes,
                       double approxDistinct,
                       DataType<T> type,
                       MostCommonValues mostCommonValues,
                       List<T> histogram) {
        this.nullFraction = nullFraction;
        this.averageSizeInBytes = averageSizeInBytes;
        this.approxDistinct = approxDistinct;
        this.type = type;
        this.mostCommonValues = mostCommonValues;
        this.histogram = histogram;
    }

    public ColumnStats(StreamInput in) throws IOException {
        //noinspection unchecked
        this.type = (DataType<T>) DataTypes.fromStream(in);

        this.nullFraction = in.readDouble();
        this.averageSizeInBytes = in.readDouble();
        this.approxDistinct = in.readDouble();
        Streamer<T> streamer = type.streamer();
        this.mostCommonValues = new MostCommonValues(streamer, in);
        int numHistogramValues = in.readVInt();
        ArrayList<T> histogram = new ArrayList<>(numHistogramValues);
        for (int i = 0; i < numHistogramValues; i++) {
            histogram.add(streamer.readValueFrom(in));
        }
        this.histogram = histogram;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(type, out);
        out.writeDouble(nullFraction);
        out.writeDouble(averageSizeInBytes);
        out.writeDouble(approxDistinct);
        mostCommonValues.writeTo(type.streamer(), out);
        out.writeVInt(histogram.size());
        Streamer<T> streamer = type.streamer();
        for (T o : histogram) {
            streamer.writeValueTo(out, o);
        }
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

    public MostCommonValues mostCommonValues() {
        return mostCommonValues;
    }

    public List<T> histogram() {
        return histogram;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnStats<?> that = (ColumnStats<?>) o;

        if (Double.compare(that.nullFraction, nullFraction) != 0) {
            return false;
        }
        if (Double.compare(that.averageSizeInBytes, averageSizeInBytes) != 0) {
            return false;
        }
        if (Double.compare(that.approxDistinct, approxDistinct) != 0) {
            return false;
        }
        if (!type.equals(that.type)) {
            return false;
        }
        if (!mostCommonValues.equals(that.mostCommonValues)) {
            return false;
        }
        return histogram.equals(that.histogram);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(nullFraction);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(averageSizeInBytes);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(approxDistinct);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + type.hashCode();
        result = 31 * result + mostCommonValues.hashCode();
        result = 31 * result + histogram.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ColumnStats{" +
            "nullFraction=" + nullFraction +
            ", approxDistinct=" + approxDistinct +
            ", mcv=" + Arrays.toString(mostCommonValues.values()) +
            ", frequencies=" + Arrays.toString(mostCommonValues.frequencies()) + '}';
    }


    /**
     * @param samples sorted sample values (must exclude any null values that might have been sampled)
     * @param nullCount Number of null values that have been excluded from the samples
     */
    public static <T> ColumnStats<T> fromSortedValues(List<T> samples,
                                                      DataType<T> type,
                                                      int nullCount,
                                                      long numTotalRows) {
        // This is heavily inspired by the logic in PostgreSQL (src/backend/command/analyze.c -> compute_scalar_stats)

        int notNullCount = samples.size();
        if (notNullCount == 0 && nullCount > 0) {
            // Only null values found, assume all values are null
            double nullFraction = 1.0;
            int averageWidth = type instanceof FixedWidthType fixedWidthType ? fixedWidthType.fixedSize() : 0;
            long approxDistinct = 1;
            return new ColumnStats<>(nullFraction, averageWidth, approxDistinct, type, MostCommonValues.EMPTY, List.of());
        }
        boolean isVariableLength = !(type instanceof FixedWidthType);
        int mcvTarget = Math.min(MostCommonValues.MCV_TARGET, samples.size());
        MostCommonValues.MVCCandidate[] mostCommonValueCandidates = initMCVItems(mcvTarget);
        long totalSampleValueSizeInBytes = 0;
        int numTracked = 0;
        int duplicates = 0;
        int distinctValues = 0;
        int numValuesWithDuplicates = 0;
        for (int i = 0; i < samples.size(); i++) {
            T currentValue = samples.get(i);
            if (isVariableLength) {
                totalSampleValueSizeInBytes += type.valueBytes(currentValue);
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
        double nullFraction = (double) nullCount / (double) (samples.size() + nullCount);
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
        MostCommonValues mostCommonValues = MostCommonValues.fromCandidates(
            nullFraction,
            numTracked,
            distinctValues,
            approxDistinct,
            samples,
            numTotalRows,
            mostCommonValueCandidates
        );
        return new ColumnStats<>(
            nullFraction,
            averageSizeInBytes,
            approxDistinct,
            type,
            mostCommonValues,
            generateHistogram(MostCommonValues.MCV_TARGET, removeMCVs(samples, mostCommonValueCandidates))
        );
    }

    private static <T> List<T> removeMCVs(List<T> samples, MostCommonValues.MVCCandidate[] mostCommonValueCandidates) {
        Arrays.sort(mostCommonValueCandidates, Comparator.comparingInt(x -> x.first));
        // The candidates are now sorted by the start indices of the most common values, e.g.
        // [0, 28, 40]
        // Together with the count we've the boundaries:
        // [0-17, 28-35, 40-48]
        // We want to have a list of samples that contain everything outside the boundaries.
        ArrayList<T> prunedSamples = new ArrayList<>();
        int i = 0;
        for (MostCommonValues.MVCCandidate mostCommonValueCandidate : mostCommonValueCandidates) {
            if (mostCommonValueCandidate.count == 0) {
                continue;
            }
            for (int j = i; j < mostCommonValueCandidate.first; j++) {
                prunedSamples.add(samples.get(j));
            }
            i = mostCommonValueCandidate.first + mostCommonValueCandidate.count;
        }
        for (int j = i; j < samples.size(); j++) {
            prunedSamples.add(samples.get(j));
        }
        return prunedSamples;
    }

    static <T> List<T> generateHistogram(int numBins, List<T> sortedValues) {
        int numHist = Math.min(numBins, sortedValues.size());
        if (numHist < 2) {
            return List.of();
        }
        ArrayList<T> histogram = new ArrayList<>(numHist);
        int delta = (sortedValues.size() - 1) / (numHist - 1);
        int position = 0;
        for (int i = 0; i < numHist; i++) {
            histogram.add(sortedValues.get(position));
            position += delta;
        }
        return histogram;
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
        /* From PostgreSQL:
         *
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
        //noinspection UnnecessaryLocalVariable
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

    private static void updateMostCommonValues(MostCommonValues.MVCCandidate[] mostCommonValueCandidates,
                                               int numTracked,
                                               int duplicates,
                                               int i) {
        // If the new candidate has more duplicates than previously tracked MCVs we insert it and bubble others down
        int j;
        for (j = numTracked - 1; j > 0; j--) {
            if (duplicates <= mostCommonValueCandidates[j - 1].count) {
                break;
            }
            mostCommonValueCandidates[j].count = mostCommonValueCandidates[j - 1].count;
            mostCommonValueCandidates[j].first = mostCommonValueCandidates[j - 1].first;
        }
        mostCommonValueCandidates[j].count = duplicates;
        mostCommonValueCandidates[j].first = i - duplicates;
    }

    private static MostCommonValues.MVCCandidate[] initMCVItems(int mcvTarget) {
        MostCommonValues.MVCCandidate[] trackedValues = new MostCommonValues.MVCCandidate[mcvTarget];
        for (int i = 0; i < trackedValues.length; i++) {
            trackedValues[i] = new MostCommonValues.MVCCandidate();
        }
        return trackedValues;
    }

}
