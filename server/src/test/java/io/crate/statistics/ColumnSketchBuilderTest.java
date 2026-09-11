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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

public class ColumnSketchBuilderTest extends ESTestCase {

    public void test_array_stats() {

        var builder = new ColumnSketchBuilder.Composite<>(
            new ArrayType<>(ObjectType.of(ColumnPolicy.DYNAMIC).setInnerType("id", IntegerType.INSTANCE).build())
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

    public void test_empty_sketch() {
        var stats = StatsUtils.statsFromValues(LongType.INSTANCE, List.of());
        assertThat(stats.approxDistinct()).isEqualTo(0);
        var arrayStats = StatsUtils.statsFromValues(new ArrayType<>(LongType.INSTANCE), List.of());
        assertThat(arrayStats.approxDistinct()).isEqualTo(0);
    }

    public void test_streaming() throws IOException {
        var support = StringType.INSTANCE.columnStatsSupport();
        var sketch = support.sketchBuilder();

        for (int i = 0; i < 200; i++) {
            sketch.add(RandomStrings.randomUnicodeOfLength(random(), 10));
        }

        assertStreamOutInOk(sketch, support, Version.V_6_5_0);
        assertStreamOutInOk(sketch, support, Version.V_6_4_4);
        assertStreamOutInOk(sketch, support, Version.V_6_4_3);
    }

    private void assertStreamOutInOk(ColumnSketchBuilder<String> sketch,
                                     ColumnStatsSupport<String> support,
                                     Version version) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(os);
        out.setVersion(version);
        sketch.writeTo(out);

        StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(os.toByteArray()));
        in.setVersion(version);
        var serializedSketch = support.readSketchFrom(in);

        assertThat(sketch.toStats(sketch.sampleCount))
            .as("version: " + version)
            .isEqualTo(serializedSketch.toStats(serializedSketch.sampleCount));
        // We stopped relying on `distinctSketch` in 6.5. However, to support
        // the ANALYZE statement in mixed clusters, `distinctSketch` needs to
        // store the correct info.
        // Also see: https://github.com/crate/crate/pull/20172.
        assertThat(serializedSketch.distinctSketch.getSketch().getEstimate())
            .as("version: " + version)
            .isEqualTo(sketch.toStats(sketch.sampleCount).approxDistinct());
    }

}
