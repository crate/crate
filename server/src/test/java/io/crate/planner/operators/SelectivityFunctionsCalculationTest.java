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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;


public class SelectivityFunctionsCalculationTest extends CrateDummyClusterServiceUnitTest {


    @Test
    public void test_collect_operator_adapts_expected_row_count_based_on_selectivity_calculation() throws Throwable {
        var columnStats = new HashMap<ColumnIdent, ColumnStats>();
        long totalNumRows = 20000;
        var numbers = IntStream.range(1, 20001)
            .boxed()
            .collect(Collectors.toList());
        columnStats.put(
            new ColumnIdent("x"),
            ColumnStats.fromSortedValues(numbers, DataTypes.INTEGER, 0, totalNumRows)
        );
        Stats stats = new Stats(totalNumRows, DataTypes.INTEGER.fixedSize(), columnStats);
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(new RelationName("doc", "tbl"), stats));
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .setTableStats(tableStats)
            .addTable("create table doc.tbl (x int)")
            .build();

        LogicalPlan plan = e.logicalPlan("select * from doc.tbl where x = 10");
        assertThat(plan.numExpectedRows(), Matchers.is(1L));
    }


    @Test
    public void test_group_operator_adapt_expected_row_count_based_on_column_stats() throws Throwable {
        var samples = IntStream.concat(
            IntStream.generate(() -> 10).limit(50),
            IntStream.generate(() -> 20).limit(50)
        ).boxed().collect(Collectors.toList());
        long numDocs = 2_000L;
        Stats stats = new Stats(
            numDocs,
            DataTypes.INTEGER.fixedSize(),
            Map.of(new ColumnIdent("x"), ColumnStats.fromSortedValues(samples, DataTypes.INTEGER, 0, numDocs))
        );
        TableStats tableStats = new TableStats();
        tableStats.updateTableStats(Map.of(new RelationName("doc", "tbl"), stats));
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .setTableStats(tableStats)
            .addTable("create table doc.tbl (x int)")
            .build();

        LogicalPlan plan = e.logicalPlan("select x, count(*) from doc.tbl group by x");
        assertThat(plan.numExpectedRows(), Matchers.is(2L));
    }
}
