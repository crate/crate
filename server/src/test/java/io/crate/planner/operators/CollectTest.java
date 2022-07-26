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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class CollectTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_prune_output_of_collect_updates_estimated_row_size() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t (x int, y int)")
            .build();
        TableStats tableStats = new TableStats();
        Symbol x = e.asSymbol("x");
        Collect collect = Collect.create(
            new DocTableRelation(e.resolveTableInfo("t")),
            List.of(x, e.asSymbol("y")),
            WhereClause.MATCH_ALL,
            tableStats,
            Row.EMPTY
        );
        assertThat(collect.estimatedRowSize(), is(DataTypes.INTEGER.fixedSize() * 2L));
        LogicalPlan prunedCollect = collect.pruneOutputsExcept(tableStats, List.of(x));
        assertThat(prunedCollect.estimatedRowSize(), is((long) DataTypes.INTEGER.fixedSize()));
    }

    @Test
    public void test() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t (x int)")
            .build();
        TableStats tableStats = new TableStats();
        PlannerContext plannerCtx = e.getPlannerContext(clusterService.state());
        ProjectionBuilder projectionBuilder = new ProjectionBuilder(e.nodeCtx);
        QueriedSelectRelation analyzedRelation = e.analyze("SELECT 123 AS alias, 456 AS alias2 FROM t ORDER BY alias, 2");
        LogicalPlanner logicalPlanner = new LogicalPlanner(
            e.nodeCtx,
            tableStats,
            () -> clusterService.state().nodes().getMinNodeVersion()
        );
        LogicalPlan operator = logicalPlanner.plan(analyzedRelation, plannerCtx);
        ExecutionPlan build = operator.build(
            mock(DependencyCarrier.class),
            plannerCtx,
            Set.of(),
            projectionBuilder,
            -1,
            0,
            null,
            null,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        assertThat((((RoutedCollectPhase) ((io.crate.planner.node.dql.Collect) build).collectPhase())).orderBy(), is(nullValue()));
    }
}
