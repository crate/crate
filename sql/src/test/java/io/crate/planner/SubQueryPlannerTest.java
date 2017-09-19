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

import io.crate.analyze.symbol.Function;
import io.crate.operation.scalar.arithmetic.ArithmeticFunctions;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SubQueryPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).addDocTable(T3.T2_INFO).build();
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    public void testNestedSimpleSelectUsesFetch() throws Exception {
        QueryThenFetch qtf = e.plan(
            "select x, i from (select x, i from t1 order by x asc limit 10) ti order by x desc limit 3");
        Collect collect = (Collect) qtf.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, Matchers.contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(FetchProjection.class)
        ));

        // Assert that the OrderedTopNProjection has correct outputs
        assertThat(projections.get(2).outputs(), isSQL("INPUT(0), INPUT(1)"));
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    public void testNestedSimpleSelectWithEarlyFetchBecauseOfWhereClause() throws Exception {
        QueryThenFetch qtf = e.plan(
            "select x, i from (select x, i from t1 order by x asc limit 10) ti where ti.i = 10 order by x desc limit 3");
        Collect collect = (Collect) qtf.subPlan();
        assertThat(collect.collectPhase().projections(), Matchers.contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(FetchProjection.class),
            instanceOf(FilterProjection.class),

            // order by is on query symbol but LIMIT must be applied after WHERE
            instanceOf(OrderedTopNProjection.class)
        ));
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    public void testTwoLevelFetchPropagation() throws Exception {
        QueryThenFetch qtf = e.plan("select x, i, a from (" +
                                    "    select a, i, x from (" +
                                    "        select x, i, a from t1 order by x asc limit 100" +
                                    "    ) tt " +
                                    "    order by tt.x desc limit 50" +
                                    ") ttt " +
                                    "order by ttt.x asc limit 10");
        Collect collect = (Collect) qtf.subPlan();
        assertThat(collect.collectPhase().projections(), Matchers.contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(FetchProjection.class),
            instanceOf(EvalProjection.class)
        ));
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    public void testSimpleSubSelectWithLateFetchWhereClauseMatchesQueryColumn() throws Exception {
        QueryThenFetch qtf = e.plan(
            "select xx, i from (select x + x as xx, i from t1 order by x asc limit 10) ti " +
            "where ti.xx = 10 order by xx desc limit 3");
        Collect collect = (Collect) qtf.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, Matchers.contains(
            instanceOf(TopNProjection.class),
            instanceOf(TopNProjection.class),
            instanceOf(FilterProjection.class),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(FetchProjection.class)
        ));
        FilterProjection filterProjection = (FilterProjection) projections.get(2);
        // filter is before fetch; preFetchOutputs: [_fetchId, x]
        assertThat(filterProjection.query(), isSQL("(add(INPUT(1), INPUT(1)) = 10)"));
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    public void testNestedSimpleSelectContainsFilterProjectionForWhereClause() throws Exception {
        QueryThenFetch qtf = e.plan("select x, i from " +
                                    "   (select x, i from t1 order by x asc limit 10) ti " +
                                    "where ti.x = 10 " +
                                    "order by x desc limit 3");
        Collect collect = (Collect) qtf.subPlan();
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
            instanceOf(TopNProjection.class),
            instanceOf(FetchProjection.class),
            instanceOf(OrderedTopNProjection.class)));

        // Assert that the OrderedTopNProjections have correct outputs
        assertThat(projections.get(0).outputs(), isSQL("INPUT(0), INPUT(1)"));
        assertThat(projections.get(2).outputs(), isSQL("INPUT(0)"));
    }

    @Test
    public void testNestedSimpleSelectContainsGroupProjectionWithFunction() throws Exception {
        Merge merge = e.plan("select c + 100, max(max) from " +
                             "    (select x + 10 as c, max(i) as max from t1 group by x + 10) t " +
                             "group by c + 100 order by c + 100 " +
                             "limit 100");
        // We assume that an add function is present in the group projection keys.
        List<Projection> projections = merge.mergePhase().projections();
        GroupProjection projection = (GroupProjection) projections.get(2);
        Function function = (Function) projection.keys().get(0);

        assertEquals(ArithmeticFunctions.Names.ADD, function.info().ident().name());
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    @SuppressWarnings("ConstantConditions")
    public void testJoinOnSubSelectsWithLimitAndOffset() throws Exception {
        NestedLoop nl = e.plan("select * from " +
                               " (select i, a from t1 order by a limit 5 offset 2) t1 " +
                               "join" +
                               " (select i from t2 order by b limit 10 offset 5) t2 " +
                               "on t1.i = t2.i");
        assertThat(nl.nestedLoopPhase().projections().size(), is(1));
        assertThat(nl.nestedLoopPhase().projections().get(0), instanceOf(EvalProjection.class));
        assertThat(nl.nestedLoopPhase().leftMergePhase(), notNullValue());
        assertThat(nl.nestedLoopPhase().rightMergePhase(), notNullValue());
        assertThat(nl.nestedLoopPhase().leftMergePhase().projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topNProjection = (TopNProjection) nl.nestedLoopPhase().leftMergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(5));
        assertThat(topNProjection.offset(), is(2));
        assertThat(nl.nestedLoopPhase().rightMergePhase().projections().get(0), instanceOf(TopNProjection.class));
        topNProjection = (TopNProjection) nl.nestedLoopPhase().rightMergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(10));
        assertThat(topNProjection.offset(), is(5));

        Collect leftPlan = (Collect) nl.left();
        assertThat(leftPlan.orderBy(), isSQL("OrderByPositions{indices=[1], reverseFlags=[false], nullsFirst=[null]}"));
        assertThat(leftPlan.limit(), is(5));
        assertThat(leftPlan.offset(), is(2));
        assertThat(leftPlan.collectPhase().projections().size(), is(1));
        topNProjection = (TopNProjection) leftPlan.collectPhase().projections().get(0);
        assertThat(topNProjection.limit(), is(7));
        assertThat(topNProjection.offset(), is(0));

        Collect rightPlan = (Collect) nl.right();
        assertThat(rightPlan.orderBy(), isSQL("OrderByPositions{indices=[1], reverseFlags=[false], nullsFirst=[null]}"));
        assertThat(rightPlan.limit(), is(10));
        assertThat(rightPlan.offset(), is(5));
        assertThat(rightPlan.collectPhase().projections().size(), is(1));
        topNProjection = (TopNProjection) rightPlan.collectPhase().projections().get(0);
        assertThat(topNProjection.limit(), is(15));
        assertThat(topNProjection.offset(), is(0));
    }


    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    @SuppressWarnings("ConstantConditions")
    public void testJoinWithAggregationOnSubSelectsWithLimitAndOffset() throws Exception {
        NestedLoop nl = e.plan("select t1.a, count(*) from " +
                               " (select i, a from t1 order by a limit 5 offset 2) t1 " +
                               "join" +
                               " (select i from t2 order by i desc limit 10 offset 5) t2 " +
                               "on t1.i = t2.i " +
                               "group by t1.a");
        assertThat(nl.nestedLoopPhase().projections().size(), is(3));
        assertThat(nl.nestedLoopPhase().projections().get(1), instanceOf(GroupProjection.class));
        assertThat(nl.nestedLoopPhase().leftMergePhase(), notNullValue());
        assertThat(nl.nestedLoopPhase().rightMergePhase(), notNullValue());
        assertThat(nl.nestedLoopPhase().leftMergePhase().projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topNProjection = (TopNProjection) nl.nestedLoopPhase().leftMergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(5));
        assertThat(topNProjection.offset(), is(2));
        assertThat(nl.nestedLoopPhase().rightMergePhase().projections().get(0), instanceOf(TopNProjection.class));
        topNProjection = (TopNProjection) nl.nestedLoopPhase().rightMergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(10));
        assertThat(topNProjection.offset(), is(5));

        Collect leftPlan = (Collect) nl.left();
        assertThat(leftPlan.orderBy(), isSQL("OrderByPositions{indices=[0], reverseFlags=[false], nullsFirst=[null]}"));
        assertThat(leftPlan.limit(), is(5));
        assertThat(leftPlan.offset(), is(2));
        assertThat(leftPlan.collectPhase().projections().size(), is(1));
        topNProjection = (TopNProjection) leftPlan.collectPhase().projections().get(0);
        assertThat(topNProjection.limit(), is(7));
        assertThat(topNProjection.offset(), is(0));

        Collect rightPlan = (Collect) nl.right();
        assertThat(rightPlan.orderBy(), isSQL("OrderByPositions{indices=[0], reverseFlags=[true], nullsFirst=[null]}"));
        assertThat(rightPlan.limit(), is(10));
        assertThat(rightPlan.offset(), is(5));
        assertThat(rightPlan.collectPhase().projections().size(), is(1));
        topNProjection = (TopNProjection) rightPlan.collectPhase().projections().get(0);
        assertThat(topNProjection.limit(), is(15));
        assertThat(topNProjection.offset(), is(0));
    }

    @Test
    @Ignore("LogicalPlanner doesn't propagate fetch yet")
    @SuppressWarnings("ConstantConditions")
    public void testJoinWithGlobalAggregationOnSubSelectsWithLimitAndOffset() throws Exception {
        NestedLoop nl = e.plan("select count(*) from " +
                               " (select i, a from t1 order by a limit 5 offset 2) t1 " +
                               "join" +
                               " (select i from t2 order by i desc limit 10 offset 5) t2 " +
                               "on t1.i = t2.i");
        assertThat(nl.nestedLoopPhase().projections().size(), is(3));
        assertThat(nl.nestedLoopPhase().projections().get(1), instanceOf(AggregationProjection.class));
        assertThat(nl.nestedLoopPhase().leftMergePhase(), notNullValue());
        assertThat(nl.nestedLoopPhase().rightMergePhase(), notNullValue());
        assertThat(nl.nestedLoopPhase().leftMergePhase().projections().get(0), instanceOf(TopNProjection.class));
        TopNProjection topNProjection = (TopNProjection) nl.nestedLoopPhase().leftMergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(5));
        assertThat(topNProjection.offset(), is(2));
        assertThat(nl.nestedLoopPhase().rightMergePhase().projections().get(0), instanceOf(TopNProjection.class));
        topNProjection = (TopNProjection) nl.nestedLoopPhase().rightMergePhase().projections().get(0);
        assertThat(topNProjection.limit(), is(10));
        assertThat(topNProjection.offset(), is(5));

        Collect leftPlan = (Collect) nl.left();
        assertThat(leftPlan.orderBy(), isSQL("OrderByPositions{indices=[1], reverseFlags=[false], nullsFirst=[null]}"));
        assertThat(leftPlan.limit(), is(5));
        assertThat(leftPlan.offset(), is(2));
        assertThat(leftPlan.collectPhase().projections().size(), is(1));
        topNProjection = (TopNProjection) leftPlan.collectPhase().projections().get(0);
        assertThat(topNProjection.limit(), is(7));
        assertThat(topNProjection.offset(), is(0));

        Collect rightPlan = (Collect) nl.right();
        assertThat(rightPlan.orderBy(), isSQL("OrderByPositions{indices=[0], reverseFlags=[true], nullsFirst=[null]}"));
        assertThat(rightPlan.limit(), is(10));
        assertThat(rightPlan.offset(), is(5));
        assertThat(rightPlan.collectPhase().projections().size(), is(1));
        topNProjection = (TopNProjection) rightPlan.collectPhase().projections().get(0);
        assertThat(topNProjection.limit(), is(15));
        assertThat(topNProjection.offset(), is(0));
    }

    @Test
    public void testJoinWithAggregationOnSubSelectsWithAggregations() throws Exception {
        NestedLoop nl = e.plan("select t1.a, count(*) from " +
                               " (select a, count(*) as cnt from t1 group by a) t1 " +
                               "join" +
                               " (select distinct i from t2) t2 " +
                               "on t1.cnt = t2.i " +
                               "group by t1.a");
        assertThat(nl.nestedLoopPhase().projections().size(), is(3));
        assertThat(nl.nestedLoopPhase().projections().get(1), instanceOf(GroupProjection.class));
        assertThat(nl.left(), instanceOf(Merge.class));
        Merge leftPlan = (Merge) nl.left();
        assertThat(leftPlan.subPlan(), instanceOf(Collect.class));
        assertThat(((Collect)leftPlan.subPlan()).collectPhase().projections().size(), is(1));
        assertThat(((Collect)leftPlan.subPlan()).collectPhase().projections().get(0),
            instanceOf(GroupProjection.class));
        Merge rightPlan = (Merge) nl.right();
        assertThat(rightPlan.subPlan(), instanceOf(Collect.class));
        assertThat(((Collect)rightPlan.subPlan()).collectPhase().projections().size(), is(1));
        assertThat(((Collect)rightPlan.subPlan()).collectPhase().projections().get(0),
            instanceOf(GroupProjection.class));
    }
}
