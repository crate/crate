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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnSketchBuilder;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.Stats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class SelectivityFunctionsCalculationTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_collect_operator_adapts_expected_row_count_based_on_selectivity_calculation() throws Throwable {
        var columnStats = new HashMap<ColumnIdent, ColumnStats<?>>();
        long totalNumRows = 20000;
        var numbers = IntStream.range(1, 20001)
            .boxed()
            .collect(Collectors.toList());
        ColumnSketchBuilder<Integer> sketchBuilder = new ColumnSketchBuilder<>(DataTypes.INTEGER);
        sketchBuilder.addAll(numbers);
        columnStats.put(
            new ColumnIdent("x"),
            sketchBuilder.toSketch().toColumnStats()
        );
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.tbl (x int)")
            .build();

        Stats stats = new Stats(totalNumRows, DataTypes.INTEGER.fixedSize(), columnStats);
        e.updateTableStats(Map.of(new RelationName("doc", "tbl"), stats));

        LogicalPlan plan = e.logicalPlan("select * from doc.tbl where x = 10");
        assertThat(e.getStats(plan).numDocs()).isEqualTo(1L);
    }

    @Test
    public void test_group_operator_adapt_expected_row_count_based_on_column_stats() throws Throwable {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.tbl (x int)")
            .build();

        var samples = IntStream.concat(
            IntStream.generate(() -> 10).limit(50),
            IntStream.generate(() -> 20).limit(50)
        ).boxed().collect(Collectors.toList());

        long numDocs = 2_000L;
        ColumnSketchBuilder<Integer> sketchBuilder = new ColumnSketchBuilder<>(DataTypes.INTEGER);
        sketchBuilder.addAll(samples);
        Stats stats = new Stats(
            numDocs,
            DataTypes.INTEGER.fixedSize(),
            Map.of(new ColumnIdent("x"), sketchBuilder.toSketch().toColumnStats())
        );

        e.updateTableStats(Map.of(new RelationName("doc", "tbl"), stats));

        LogicalPlan plan = e.logicalPlan("select x, count(*) from doc.tbl group by x");
        assertThat(e.getStats(plan).numDocs()).isEqualTo(2L);
    }
}
