/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

public class ColumnSketchBuilderTest extends ESTestCase {

    @Test
    public void test_array_stats() {
        var builder = new ColumnSketchBuilder.Composite<>(
            new ArrayType<>(ObjectType.of(ColumnPolicy.DYNAMIC).setInnerType("id", IntegerType.INSTANCE).build()),
            Version.CURRENT
        );

        builder.add(List.of(Map.of("id", 1)));
        builder.add(List.of(Map.of("id", 2)));
        builder.add(List.of(Map.of("id", 1)));

        var stats = builder.toStats(builder.sampleCount);

        assertThat(stats.mostCommonValues().values()).containsExactly(
            List.of(Map.of("id", 1)),
            List.of(Map.of("id", 2))
        );
    }

    @Test
    public void test_empty_sketch() {
        var stats = StatsUtils.statsFromValues(LongType.INSTANCE, List.of());
        assertThat(stats.approxDistinct()).isEqualTo(0);
        var arrayStats = StatsUtils.statsFromValues(new ArrayType<>(LongType.INSTANCE), List.of());
        assertThat(arrayStats.approxDistinct()).isEqualTo(0);
    }

    @Test
    public void test_streaming() throws IOException {
        var support = StringType.INSTANCE.columnStatsSupport();
        var sketch = support.sketchBuilder(Version.CURRENT);

        for (int i = 0; i < 200; i++) {
            sketch.add(RandomStrings.randomUnicodeOfLength(random(), 10));
        }

        var serializedSketch = streamOutIn(sketch, support, Version.CURRENT);

        assertThat(sketch.toStats(sketch.sampleCount))
            .isEqualTo(serializedSketch.toStats(serializedSketch.sampleCount));
        assertThat(serializedSketch.distinctCounts).isNotNull();
    }

    @Test
    public void test_streaming_pre_6_5_0() throws IOException {
        var support = StringType.INSTANCE.columnStatsSupport();
        var sketch = support.sketchBuilder(Version.V_6_5_0);

        for (int i = 0; i < 200; i++) {
            sketch.add(RandomStrings.randomUnicodeOfLength(random(), 10));
        }

        var serializedSketch = streamOutIn(sketch, support, Version.V_6_5_0);

        assertThat(sketch.toStats(sketch.sampleCount))
            .isEqualTo(serializedSketch.toStats(serializedSketch.sampleCount));
        // We stopped relying on `distinctSketch` in 6.5.1.
        assertThat(serializedSketch.distinctSketch).isNotNull();
    }

    @Test
    public void test_streaming_distinct_value_set() throws IOException {
        var support = StringType.INSTANCE.columnStatsSupport();
        var sketch = support.sketchBuilder(Version.CURRENT);
        for (int i = 0; i < 200; i++) {
            String value = RandomStrings.randomUnicodeOfLength(random(), 10);
            sketch.add(value);
            if (i % 2 == 0) {
                sketch.add(value);
            }
        }

        var serializedSketch = streamOutIn(sketch, support, Version.CURRENT);

        assertThat(serializedSketch.distinctSketch).isNull();
        assertThat(serializedSketch.toStats(10_000)).isEqualTo(sketch.toStats(10_000));
    }

    @Test
    public void test_merge() {
        for (Version version : List.of(Version.V_6_5_0, Version.CURRENT)) {
            var support = IntegerType.INSTANCE.columnStatsSupport();
            var builder1 = support.sketchBuilder(version);
            var builder2 = support.sketchBuilder(version);

            for (int j = 1; j <= 100; j++) {
                builder1.add(j);
            }
            for (int j = 51; j <= 150; j++) {
                builder2.add(j);
            }

            var merged = builder1.merge(builder2);

            assertThat(merged.toStats(merged.sampleCount).approxDistinct())
                .as("version: " + version)
                .isEqualTo(150.0);
        }
    }

    private static ColumnSketchBuilder<String> streamOutIn(ColumnSketchBuilder<String> sketch,
                                                           ColumnStatsSupport<String> support,
                                                           Version version) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(os);
        out.setVersion(version);
        sketch.writeTo(out);

        StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(os.toByteArray()));
        in.setVersion(version);
        return support.readSketchFrom(version, in);
    }
}
