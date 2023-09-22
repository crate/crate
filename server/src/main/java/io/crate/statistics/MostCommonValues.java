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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public final class MostCommonValues {

    public static final MostCommonValues EMPTY = new MostCommonValues(new Object[0], new double[0]);
    static final int MCV_TARGET = 100;

    private final Object[] values;
    private final double[] frequencies;

    static <T> MostCommonValues fromCandidates(double nullFraction,
                                               int numTracked,
                                               int distinctValues,
                                               double approxDistinct,
                                               List<T> samples,
                                               long numTotalRows,
                                               MVCCandidate[] candidates) {

        /* From PostgreSQL:
         *
         * Decide how many values are worth storing as most-common values. If
         * we are able to generate a complete MCV list (all the values in the
         * sample will fit, and we think these are all the ones in the table),
         * then do so.  Otherwise, store only those values that are
         * significantly more common than the values not in the list.
         *
         * Note: the first of these cases is meant to address columns with
         * small, fixed sets of possible values, such as boolean or enum
         * columns.  If we can *completely* represent the column population by
         * an MCV list that will fit into the stats target, then we should do
         * so and thus provide the planner with complete information.  But if
         * the MCV list is not complete, it's generally worth being more
         * selective, and not just filling it all the way up to the stats
         * target.
         */
        int numMcv = MCV_TARGET;
        if (numTracked == distinctValues && approxDistinct > 0 && numTracked <= numMcv) {
            numMcv = numTracked;
        } else {
            if (numMcv > numTracked) {
                numMcv = numTracked;
            }
            if (numMcv > 0) {
                int[] mcvCounts = new int[numMcv];
                for (int i = 0; i < numMcv; i++) {
                    mcvCounts[i] = candidates[i].count;
                }
                numMcv = decodeHowManyCommonValuesToKeep(
                    mcvCounts, numMcv, approxDistinct, nullFraction, samples.size(), numTotalRows);
            }
        }
        if (numMcv == 0) {
            return EMPTY;
        }
        Object[] values = new Object[numMcv];
        double[] frequencies = new double[numMcv];
        for (int i = 0; i < numMcv; i++) {
            values[i] = samples.get(candidates[i].first);
            frequencies[i] = (double) candidates[i].count / (double) samples.size();
        }
        return new MostCommonValues(values, frequencies);
    }

    private static int decodeHowManyCommonValuesToKeep(int[] mcvCounts,
                                                       int numMcv,
                                                       double approxDistinct,
                                                       double nullFraction,
                                                       int numSampleRows,
                                                       long numTotalRows) {
        assert mcvCounts.length == numMcv : "mcvCounts.length must be equal to numMcv";
        if (numSampleRows == numTotalRows || numTotalRows <= 1) {
            // Entire table was sampled, keep all candidates
            return numMcv;
        }
        /* From PostgreSQL:
         *
         * Exclude the least common values from the MCV list, if they are not
         * significantly more common than the estimated selectivity they would
         * have if they weren't in the list.  All non-MCV values are assumed to be
         * equally common, after taking into account the frequencies of all the
         * values in the MCV list and the number of nulls (c.f. eqsel()).
         *
         * Here sumcount tracks the total count of all but the last (least common)
         * value in the MCV list, allowing us to determine the effect of excluding
         * that value from the list.
         *
         * Note that we deliberately do this by removing values from the full
         * list, rather than starting with an empty list and adding values,
         * because the latter approach can fail to add any values if all the most
         * common values have around the same frequency and make up the majority
         * of the table, so that the overall average frequency of all values is
         * roughly the same as that of the common values.  This would lead to any
         * uncommon values being significantly overestimated.
         */
        int sumCount = 0;
        for (int i = 0; i < numMcv - 1; i++) {
            sumCount += mcvCounts[i];
        }
        while (numMcv > 0) {
            /*
             * Estimated selectivity the least common value would have if it
             * wasn't in the MCV list (c.f. eqsel()).
             */
            double selectivity = 1.0 - (double) sumCount / numSampleRows - nullFraction;
            selectivity = Math.max(0.0, Math.min(1.0, selectivity));
            double otherDistinct = approxDistinct - (numMcv - 1);
            if (otherDistinct > 1) {
                selectivity /= otherDistinct;
            }
            /*
             * If the value is kept in the MCV list, its population frequency is
             * assumed to equal its sample frequency.  We use the lower end of a
             * textbook continuity-corrected Wald-type confidence interval to
             * determine if that is significantly more common than the non-MCV
             * frequency --- specifically we assume the population frequency is
             * highly likely to be within around 2 standard errors of the sample
             * frequency, which equates to an interval of 2 standard deviations
             * either side of the sample count, plus an additional 0.5 for the
             * continuity correction.  Since we are sampling without replacement,
             * this is a hypergeometric distribution.
             *
             * XXX: Empirically, this approach seems to work quite well, but it
             * may be worth considering more advanced techniques for estimating
             * the confidence interval of the hypergeometric distribution.
             */
            //noinspection UnnecessaryLocalVariable
            double N = numTotalRows;
            //noinspection UnnecessaryLocalVariable
            double n = numSampleRows;
            double K = N * mcvCounts[numMcv - 1] / n;
            double variance = n * K * (N - K) * (N - n) / (N * N * (N - 1));
            double stddev = Math.sqrt(variance);

            if (mcvCounts[numMcv - 1] > selectivity * numSampleRows + 2 * stddev + 0.5) {
                /*
                 * The value is significantly more common than the non-MCV
                 * selectivity would suggest.  Keep it, and all the other more
                 * common values in the list.
                 */
                break;
            } else {
                /* Discard this value and consider the next least common value */
                numMcv--;
                if (numMcv == 0)
                    break;
                sumCount -= mcvCounts[numMcv - 1];
            }
        }
        return numMcv;
    }

    public MostCommonValues(Object[] values, double[] frequencies) {
        assert values.length == frequencies.length : "values and frequencies must have the same number of items";
        this.values = values;
        this.frequencies = frequencies;
    }

    @SuppressWarnings("rawtypes")
    public MostCommonValues(Streamer valueStreamer, StreamInput in) throws IOException {
        int numValues = in.readVInt();
        values = new Object[numValues];
        frequencies = new double[numValues];
        for (int i = 0; i < numValues; i++) {
            values[i] = valueStreamer.readValueFrom(in);
            frequencies[i] = in.readDouble();
        }

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void writeTo(Streamer valueStreamer, StreamOutput out) throws IOException {
        out.writeVInt(values.length);
        for (int i = 0; i < values.length; i++) {
            valueStreamer.writeValueTo(out, values[i]);
            out.writeDouble(frequencies[i]);
        }
    }

    public boolean isEmpty() {
        return values.length == 0;
    }

    public int length() {
        return values.length;
    }

    public Object value(int index) {
        return values[index];
    }

    public double frequency(int index) {
        return frequencies[index];
    }

    public Object[] values() {
        return values;
    }

    public double[] frequencies() {
        return frequencies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MostCommonValues that = (MostCommonValues) o;

        if (!Arrays.equals(values, that.values)) {
            return false;
        }
        return Arrays.equals(frequencies, that.frequencies);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(values);
        result = 31 * result + Arrays.hashCode(frequencies);
        return result;
    }

    static class MVCCandidate {

        int first = 0;
        int count = 0;

        @Override
        public String toString() {
            return "MVCCandidate{" +
                   "first=" + first +
                   ", count=" + count +
                   '}';
        }
    }
}
