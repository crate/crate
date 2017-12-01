/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner;

import io.crate.analyze.TableDefinitions;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class UnionPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_INFO)
            .build();
    }

    @Test
    public void testSimpleUnion() {
        ExecutionPlan plan = e.plan(
            "select id from users " +
            "union all " +
            "select id from locations ");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.orderBy(), is(nullValue()));
        assertThat(unionExecutionPlan.mergePhase().numInputs(), is(2));
        assertThat(unionExecutionPlan.left(), instanceOf(Collect.class));
        assertThat(unionExecutionPlan.right(), instanceOf(Collect.class));
    }

    @Test
    public void testUnionWithOrderByLimit() {
        ExecutionPlan plan = e.plan(
            "select id from users " +
            "union all " +
            "select id from locations " +
            "order by id limit 2");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().numInputs(), is(2));
        assertThat(unionExecutionPlan.mergePhase().orderByPositions(), instanceOf(PositionalOrderBy.class));
        assertThat(unionExecutionPlan.mergePhase().projections(), contains(
            instanceOf(TopNProjection.class)
        ));
        assertThat(unionExecutionPlan.left(), instanceOf(Collect.class));
        assertThat(unionExecutionPlan.right(), instanceOf(Collect.class));
    }

    @Test
    public void testUnionWithSubselects() {
        ExecutionPlan plan = e.plan(
            "select * from (select id from users order by id limit 2) a " +
            "union all " +
            "select id from locations " +
            "order by id limit 2");
        assertThat(plan, instanceOf(UnionExecutionPlan.class));
        UnionExecutionPlan unionExecutionPlan = (UnionExecutionPlan) plan;
        assertThat(unionExecutionPlan.mergePhase().numInputs(), is(2));
        assertThat(unionExecutionPlan.orderBy(), is(nullValue()));
        assertThat(unionExecutionPlan.mergePhase().projections(), contains(
            instanceOf(TopNProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(TopNProjection.class)
        ));
        assertThat(unionExecutionPlan.left(), instanceOf(Merge.class));
        Merge merge = (Merge) unionExecutionPlan.left();
        assertThat(merge.subPlan(), instanceOf(Collect.class));
        assertThat(unionExecutionPlan.right(), instanceOf(Collect.class));
    }
}
