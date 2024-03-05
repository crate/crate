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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

import io.crate.types.DataTypes;

@RunWith(JUnitQuickcheck.class)
public class ColumnStatsTest {

    @Test
    public void test_column_stats_generation() {
        ColumnStats columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, List.of(1, 1, 2, 4, 4, 4, 4, 4));
        assertThat(columnStats.averageSizeInBytes(), is((double) DataTypes.INTEGER.fixedSize()));
        assertThat(columnStats.nullFraction(), Matchers.closeTo(0.111, 0.01));
        assertThat(columnStats.approxDistinct(), is(3.0));
        MostCommonValues mostCommonValues = columnStats.mostCommonValues();
        assertThat(mostCommonValues.values().length, is(0));
    }

    @Property
    public void test_null_fraction_is_between_incl_0_and_incl_1(ArrayList<Integer> numbers,
                                                                @InRange(minInt = 0) int nullCount,
                                                                long totalRowCount) {
        assumeThat(totalRowCount, greaterThanOrEqualTo((long) nullCount));
        assumeThat((long) numbers.size() + nullCount, lessThanOrEqualTo(totalRowCount));

        ColumnStats<Integer> stats = StatsUtils.statsFromValues(DataTypes.INTEGER, numbers);
        assertThat(stats.nullFraction(), greaterThanOrEqualTo(0.0));
        assertThat(stats.nullFraction(), lessThanOrEqualTo(1.0));
    }

    @Test
    public void test_common_stats_can_be_serialized_and_deserialized() throws IOException {
        List<Integer> numbers = List.of(1, 1, 2, 4, 4, 4, 4, 4);
        ColumnStats columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, numbers);

        BytesStreamOutput out = new BytesStreamOutput();
        columnStats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ColumnStats stats = new ColumnStats(in);

        assertThat(stats, is(columnStats));
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
            .collect(Collectors.toList());

        ColumnStats<Integer> columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, numbers);
        List<Integer> histogramSample = columnStats.histogram().subList(0, 15);
        assertThat(histogramSample, contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 30, 31, 32, 33, 34, 35));
        MostCommonValues mostCommonValues = columnStats.mostCommonValues();
        assertThat(mostCommonValues.values().length, is(2));
        assertThat(mostCommonValues.values()[0], is(10));
        assertThat(mostCommonValues.frequencies()[0], Matchers.closeTo(0.376, 0.01));
        assertThat(mostCommonValues.values()[1], is(20));
        assertThat(mostCommonValues.frequencies()[1], Matchers.closeTo(0.086, 0.01));
    }

    @Test
    public void test_histogram_contains_evenly_spaced_values_from_samples() {
        HistogramSketch<Integer> sketch = new HistogramSketch<>(DataTypes.INTEGER);
        IntStream.range(1, 21).boxed().forEach(sketch::update);
        List<Integer> histogram = sketch.toHistogram();
        assertThat(histogram, contains(1, 7, 13, 19));
    }

    @Test
    public void test_average_size_in_bytes_is_calculated_for_variable_width_columns() {
        ColumnStats stats = StatsUtils.statsFromValues(DataTypes.STRING, List.of("a", "b", "ccc", "dddddd"));
        assertThat(stats.averageSizeInBytes(), is(50.0));
    }

    @Test
    public void test_column_stats_generation_from_null_values_only_has_null_fraction_1() {
        ColumnStats columnStats = StatsUtils.statsFromValues(DataTypes.INTEGER, List.of());
        assertThat(columnStats.nullFraction(), is(1.0));
    }
}
