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

import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;
import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_OFFSET;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isLimitAndOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Set;

import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.TableDefinitions;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Literal;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class LimitTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testLimitOnLimitOperator() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
        QueriedSelectRelation queriedDocTable = e.analyze("select name from users");

        LogicalPlan plan = Limit.create(
            Limit.create(
                new Collect(
                    ((AbstractTableRelation<?>) queriedDocTable.from().get(0)),
                    queriedDocTable.outputs(),
                    new WhereClause(queriedDocTable.where())
                ),
                Literal.of(10L),
                Literal.of(5L)
            ),
            Literal.of(20L),
            Literal.of(7L)
        );
        assertThat(plan).isEqualTo(
            """
            Limit[20::bigint;7::bigint]
              └ Limit[10::bigint;5::bigint]
                └ Collect[doc.users | [name] | true]
            """
        );
        PlannerContext ctx = e.getPlannerContext();
        Merge merge = (Merge) plan.build(
            mock(DependencyCarrier.class),
            ctx,
            Set.of(),
            new ProjectionBuilder(e.nodeCtx),
            NO_LIMIT,
            NO_OFFSET,
            null,
            null,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
        io.crate.planner.node.dql.Collect collect = (io.crate.planner.node.dql.Collect) merge.subPlan();
        assertThat(collect.collectPhase().projections()).satisfiesExactly(
            isLimitAndOffset(15, 0));
        assertThat(merge.mergePhase().projections()).satisfiesExactly(
            isLimitAndOffset(10, 5),
            isLimitAndOffset(20, 7));
    }

    @Test
    public void test_no_limit_and_no_offset_on_limit_operator() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
        QueryThenFetch qtf = e.plan("SELECT * FROM users LIMIT null OFFSET 0");
        assertThat(qtf.subPlan()).isExactlyInstanceOf(io.crate.planner.node.dql.Collect.class);
        io.crate.planner.node.dql.Collect collect = (io.crate.planner.node.dql.Collect) qtf.subPlan();
        assertThat(collect.limit()).isEqualTo(NO_LIMIT);
        assertThat(collect.offset()).isEqualTo(NO_OFFSET);
        assertThat(collect.collectPhase().projections()).hasSize(1);
        assertThat(collect.collectPhase().projections().get(0)).isExactlyInstanceOf(FetchProjection.class);
    }

    @Test
    public void test_no_limit_with_offset_on_limit_operator() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
        QueryThenFetch qtf = e.plan("SELECT * FROM users LIMIT null OFFSET 10");
        assertThat(qtf.subPlan()).isExactlyInstanceOf(io.crate.planner.node.dql.Collect.class);
        io.crate.planner.node.dql.Collect collect = (io.crate.planner.node.dql.Collect) qtf.subPlan();
        assertThat(collect.limit()).isEqualTo(NO_LIMIT);
        assertThat(collect.offset()).isEqualTo(NO_OFFSET);
        assertThat(collect.collectPhase().projections()).hasSize(2);
        assertThat(collect.collectPhase().projections().get(0)).isExactlyInstanceOf(LimitAndOffsetProjection.class);
        assertThat(collect.collectPhase().projections().get(1)).isExactlyInstanceOf(FetchProjection.class);
        assertThat(collect.collectPhase().projections().get(0)).isLimitAndOffset(NO_LIMIT, 10);
    }
}
