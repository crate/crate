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

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.Literal;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.statistics.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.ProjectionMatchers;
import io.crate.testing.SQLExecutor;

public class LimitTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testLimitOnLimitOperator() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService, 2, RandomizedTest.getRandom(), List.of())
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();
        QueriedSelectRelation queriedDocTable = e.analyze("select name from users");

        LogicalPlan plan = Limit.create(
            Limit.create(
                Collect.create(
                    ((AbstractTableRelation<?>) queriedDocTable.from().get(0)),
                    queriedDocTable.outputs(),
                    new WhereClause(queriedDocTable.where()),
                    new TableStats(),
                    null
                ),
                Literal.of(10L),
                Literal.of(5L)
            ),
            Literal.of(20L),
            Literal.of(7L)
        );
        assertThat(plan, isPlan(
            "Limit[20::bigint;7::bigint]\n" +
            "  └ Limit[10::bigint;5::bigint]\n" +
            "    └ Collect[doc.users | [name] | true]"));
        PlannerContext ctx = e.getPlannerContext(clusterService.state());
        Merge merge = (Merge) plan.build(
            mock(DependencyCarrier.class),
            ctx,
            Set.of(),
            new ProjectionBuilder(e.nodeCtx),
            LimitAndOffset.NO_LIMIT,
            0,
            null,
            null,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        io.crate.planner.node.dql.Collect collect = (io.crate.planner.node.dql.Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections(), contains(
            ProjectionMatchers.isLimitAndOffset(15, 0)
        ));
        //noinspection unchecked
        assertThat(merge.mergePhase().projections(), contains(
            ProjectionMatchers.isLimitAndOffset(10, 5),
            ProjectionMatchers.isLimitAndOffset(20, 7)
        ));
    }
}
