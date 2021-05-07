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

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

public class GroupHashAggregateTest extends CrateDummyClusterServiceUnitTest {

    private TableStats tableStats;
    private SqlExpressions expressions;

    @Before
    public void setUpStatsAndExpressions() throws Exception {
        var samples = IntStream.concat(
            IntStream.generate(() -> 10).limit(50),
            IntStream.generate(() -> 20).limit(50)
        ).boxed().collect(Collectors.toList());
        long numDocs = 2_000L;
        ColumnStats<Integer> columnStats = ColumnStats.fromSortedValues(samples, DataTypes.INTEGER, 0, numDocs);
        Stats stats = new Stats(
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
        long distinctValues = GroupHashAggregate.approximateDistinctValues(50_000L, tableStats, List.of(substringFunction));
        assertThat(distinctValues, is(50_000L));
    }

    @Test
    public void test_distinct_value_for_multiple_columns_is_the_product_of_the_distinct_values_of_each() {
        Symbol x = expressions.asSymbol("t1.x");
        Symbol i = expressions.asSymbol("t1.i");
        long distinctValues = GroupHashAggregate.approximateDistinctValues(2_000L, tableStats, List.of(x, i));
        assertThat(distinctValues, is(4L));
    }
}
