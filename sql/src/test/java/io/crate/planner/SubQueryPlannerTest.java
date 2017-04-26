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

import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.PlanWithFetchDescription;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.*;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;

public class SubQueryPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();
    }

    @Test
    public void testNestedSimpleSelectUsesFetch() throws Exception {
        QueryThenFetch qtf = e.plan(
            "select x, i from (select x, i from t1 order by x asc limit 10) ti order by x desc limit 3");
        PlanWithFetchDescription planWithFetchDescription = (PlanWithFetchDescription) qtf.subPlan();
        Collect collect = (Collect) planWithFetchDescription.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, Matchers.contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class),
            // TODO: We can optimize this to delay fetch until after the OrderedTopNProjection
            instanceOf(FetchProjection.class),
            instanceOf(OrderedTopNProjection.class)));

        // Assert that the OrderedTopNProjection has correct outputs
        assertThat(projections.get(3).outputs(), isSQL("INPUT(0), INPUT(1)"));
    }

    @Test
    public void testNestedSimpleSelectContainsFilterProjectionForWhereClause() throws Exception {
        QueryThenFetch qtf = e.plan("select x, i from " +
                                    "   (select x, i from t1 order by x asc limit 10) ti " +
                                    "where ti.x = 10 " +
                                    "order by x desc limit 3");
        PlanWithFetchDescription planWithFetchDescription = (PlanWithFetchDescription) qtf.subPlan();
        Collect collect = (Collect) planWithFetchDescription.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, Matchers.hasItem(instanceOf(FilterProjection.class)));
    }

    @Test
    public void testNestedSimpleSelectWithJoin() throws Exception {
        QueryThenFetch qtf = e.plan("select t1x from (" +
                                     "select t1.x as t1x, t2.i as t2i from t1 as t1, t1 as t2 order by t1x asc limit 10" +
                                   ") t order by t1x desc limit 3");
        List<Projection> projections = ((NestedLoop) qtf.subPlan()).nestedLoopPhase().projections();
        assertThat(projections, Matchers.contains(
            instanceOf(OrderedTopNProjection.class),
            instanceOf(FetchProjection.class),
            instanceOf(OrderedTopNProjection.class)));

        // Assert that the OrderedTopNProjections have correct outputs
        assertThat(projections.get(0).outputs(), isSQL("INPUT(0), INPUT(1)"));
        assertThat(projections.get(2).outputs(), isSQL("INPUT(0)"));
    }
}
