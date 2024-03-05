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

package io.crate.planner.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnSketchBuilder;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;

public class GroupHashAggregateTest extends CrateDummyClusterServiceUnitTest {

    private TableStats tableStats;
    private SqlExpressions expressions;
    private Stats stats;

    @Before
    public void setUpStatsAndExpressions() throws Exception {
        var samples = IntStream.concat(
            IntStream.generate(() -> 10).limit(50),
            IntStream.generate(() -> 20).limit(50)
        ).boxed().collect(Collectors.toList());
        long numDocs = 2_000L;
        ColumnSketchBuilder<Integer> sketchBuilder = new ColumnSketchBuilder<>(DataTypes.INTEGER);
        sketchBuilder.addAll(samples);
        ColumnStats<Integer> columnStats = sketchBuilder.toSketch().toColumnStats();
        stats = new Stats(
            numDocs,
            DataTypes.INTEGER.fixedSize(),
            Map.of(
                new ColumnIdent("x"), columnStats,
                new ColumnIdent("i"), columnStats
            )
        );
        tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(new RelationName("doc", "t1"), stats));
        expressions = new SqlExpressions(T3.sources(clusterService));
    }

    @Test
    public void test_distinct_value_approximation_on_functions_returns_source_row_count() {
        Symbol substringFunction = expressions.asSymbol("substr(a, 0, 1)");
        Stats stats = new Stats(50_000L, this.stats.sizeInBytes(), this.stats.statsByColumn());
        long distinctValues = GroupHashAggregate.approximateDistinctValues(stats, List.of(substringFunction));
        assertThat(distinctValues).isEqualTo(50_000L);
    }

    @Test
    public void test_distinct_value_for_multiple_columns_is_the_product_of_the_distinct_values_of_each() {
        Symbol x = expressions.asSymbol("t1.x");
        Symbol i = expressions.asSymbol("t1.i");
        Stats stats = new Stats(2_000L, this.stats.sizeInBytes(), this.stats.statsByColumn());
        long distinctValues = GroupHashAggregate.approximateDistinctValues(stats, List.of(x, i));
        assertThat(distinctValues).isEqualTo(4L);
    }
}
