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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.assertj.core.data.Offset;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.jupiter.api.Test;

import io.crate.types.DataTypes;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

public class ColumnStatsTest {

    @Test
    public void test_column_stats_generation() {
        ColumnStats<?> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, List.of(1, 1, 2, 4, 4, 4, 4, 4), 1);
        assertThat(columnStats.averageSizeInBytes()).isEqualTo((double) DataTypes.INTEGER.fixedSize());
        assertThat(columnStats.nullFraction()).isCloseTo(0.111, Offset.offset(0.01));
        assertThat(columnStats.approxDistinct()).isEqualTo(3.0);
        MostCommonValues<?> mostCommonValues = columnStats.mostCommonValues();
        assertThat(mostCommonValues.values()).hasSize(3);
    }

    @Test
    public void test_approx_distinct_is_exact_when_sample_covers_whole_table() {
        // n == N (sample is the entire table): the Haas-Stokes estimator must not distort
        // the exact distinct count, regardless of sample size relative to the 30k reservoir cap.
        List<Integer> values = IntStream.range(0, 500).boxed().toList();
        ColumnStats<?> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, values, 0, 500);
        assertThat(columnStats.approxDistinct()).isEqualTo(500.0);
    }

    @Test
    public void test_approx_distinct_extrapolates_sample_larger_than_reservoir_cap() {
        // Sample (n = 40,000) exceeds Reservoir.NUM_SAMPLES (30,000): 30,000 values occur
        // once (f1 = 30,000), 5,000 values occur twice each -> d = 35,000.
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < 30_000; i++) {
            values.add(i);
        }
        for (int i = 30_000; i < 35_000; i++) {
            values.add(i);
            values.add(i);
        }

        // Haas-Stokes: n*d / (n - f1 + f1*n/N) = 40000*35000 / (10000 + 30000*40000/400000)
        //            = 1,400,000,000 / 13,000 = 107,692.307692...
        ColumnStats<?> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, values, 0, 400_000);
        assertThat(columnStats.approxDistinct()).isEqualTo(107_692);
    }

    @Test
    public void test_approx_distinct_scales_up_when_sample_is_all_unique() {
        // Regression test
        // Earlier, ColumnStats.approxDistinct() used to report the distinct values from the sample,
        // without extrapolating to the full table.
        // This meant that, for high-cardinality columns, the reports num. of distinct values was
        // always 30k.
        List<Integer> values = IntStream.range(0, 30_000).boxed().toList();
        ColumnStats<?> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, values, 0, 5_000_000);
        assertThat(columnStats.approxDistinct()).isEqualTo(5_000_000.0);
    }

    @Property
    public void test_null_fraction_is_between_incl_0_and_incl_1(ArrayList<Integer> numbers,
                                                                @IntRange(min = 0) int nullCount,
                                                                long totalRowCount) {
        assumeThat(totalRowCount).isGreaterThanOrEqualTo(nullCount);
        assumeThat((long) numbers.size() + nullCount).isLessThanOrEqualTo(totalRowCount);

        ColumnStats<Integer> stats = StatsUtils.statsFromValues(DataTypes.INTEGER, numbers);
        assertThat(stats.nullFraction()).isGreaterThanOrEqualTo(0.0);
        assertThat(stats.nullFraction()).isLessThanOrEqualTo(1.0);
    }

    @Test
    public void test_common_stats_can_be_serialized_and_deserialized() throws IOException {
        List<Integer> numbers = List.of(1, 1, 2, 4, 4, 4, 4, 4);
        ColumnStats<?> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, numbers);

        BytesStreamOutput out = new BytesStreamOutput();
        columnStats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ColumnStats<?> stats = new ColumnStats<>(in);

        assertThat(stats).isEqualTo(columnStats);
    }

    @Test
    public void test_column_stats_generation_for_num_samples_gt_mcv_target() {
        List<Integer> numbers = IntStream.concat(
            IntStream.concat(
                IntStream.range(1, 10),
                IntStream.generate(() -> 10).limit(90)
            ),
            IntStream.concat(
                IntStream.generate(() -> 20).limit(20),
                IntStream.range(30, 150)
            ))
            .boxed()
            .toList();

        ColumnStats<Integer> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, numbers);
        List<Integer> histogramSample = columnStats.histogram().subList(0, 8);
        assertThat(histogramSample).containsExactly(1, 3, 5, 7, 9, 31, 33, 35);
        MostCommonValues<?> mostCommonValues = columnStats.mostCommonValues();
        assertThat(mostCommonValues.values()).hasSize(2);
        assertThat(mostCommonValues.values().get(0)).isEqualTo(10);
        assertThat(mostCommonValues.frequencies()[0]).isCloseTo(0.376, Offset.offset(0.01));
        assertThat(mostCommonValues.values().get(1)).isEqualTo(20);
        assertThat(mostCommonValues.frequencies()[1]).isCloseTo(0.086, Offset.offset(0.01));
    }

    @Test
    public void test_histogram_contains_evenly_spaced_values_from_samples() {
        HistogramSketch<Integer> sketch = new HistogramSketch<>(Integer.class, DataTypes.INTEGER);
        IntStream.range(1, 21).boxed().forEach(sketch::update);
        List<Integer> histogram = sketch.toHistogram(4, List.of());
        assertThat(histogram).containsExactly(1, 7, 13, 19);
    }

    @Test
    public void test_average_size_in_bytes_is_calculated_for_variable_width_columns() {
        ColumnStats<?> stats = StatsUtils.statsFromValues(DataTypes.STRING, List.of("a", "b", "ccc", "dddddd"));
        assertThat(stats.averageSizeInBytes()).isEqualTo(50.0);
    }

    @Test
    public void test_column_stats_generation_from_null_values_only_has_null_fraction_1() {
        List<Integer> allNulls = new ArrayList<>();
        allNulls.add(null);
        ColumnStats<?> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, allNulls);
        assertThat(columnStats.nullFraction()).isEqualTo(1.0);
    }
}
