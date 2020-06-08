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

import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.execution.dsl.projection.OrderedTopNProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.join.Join;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.ProjectionMatchers.isTopN;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SubQueryPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .build();
    }

    @Test
    public void testNestedSimpleSelectContainsFilterProjectionForWhereClause() throws Exception {
        QueryThenFetch qtf = e.plan(
            "select x, i from " +
            "   (select x, i from t1 order by x asc limit 10) ti " +
            "where ti.x = 10 " +
            "order by x desc limit 3");
        Collect collect = (Collect) qtf.subPlan();
        List<Projection> projections = collect.collectPhase().projections();
        assertThat(projections, Matchers.hasItem(instanceOf(FilterProjection.class)));
    }

    @Test
    public void testNestedSimpleSelectWithJoin() throws Exception {
        Join nl= e.plan("select t1x from (" +
                        "select t1.x as t1x, t2.i as t2i from t1 as t1, t1 as t2 order by t1x asc limit 10" +
                        ") t order by t1x desc limit 3");
        List<Projection> projections = nl.joinPhase().projections();
        assertThat(projections, Matchers.contains(
            instanceOf(EvalProjection.class),
            isTopN(10, 0),
            instanceOf(OrderedTopNProjection.class),
            instanceOf(EvalProjection.class),
            isTopN(3, 0)
        ));
        assertThat(projections.get(0).outputs(), isSQL("INPUT(1), INPUT(1)"));
        assertThat(projections.get(4).outputs(), isSQL("INPUT(0)"));
    }

    @Test
    public void testNestedSimpleSelectContainsGroupProjectionWithFunction() throws Exception {
        Collect collect = e.plan("select c + 100, max(max) from " +
                                 "    (select x + 10::int as c, max(i) as max from t1 group by x + 10::int) t " +
                                 "group by c + 100 order by c + 100 " +
                                 "limit 100");
        CollectPhase collectPhase = collect.collectPhase();
        assertThat(
            collectPhase.toCollect(),
            contains(
                isReference("i"),
                isFunction("add", isReference("x"), isLiteral(10))
            )
        );
        assertThat(
            collectPhase.projections(),
            contains(
                instanceOf(GroupProjection.class),
                instanceOf(GroupProjection.class),
                instanceOf(EvalProjection.class),
                instanceOf(GroupProjection.class),
                instanceOf(OrderedTopNProjection.class),
                instanceOf(TopNProjection.class)
            )
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testJoinOnSubSelectsWithLimitAndOffset() throws Exception {
        Join join = e.plan("select * from " +
                         " (select i, a from t1 order by a limit 10 offset 2) t1 " +
                         "join" +
                         " (select i from t2 order by b limit 5 offset 5) t2 " +
                         "on t1.i = t2.i");
        assertThat(join.joinPhase().projections().size(), is(1));
        assertThat(join.joinPhase().projections().get(0), instanceOf(EvalProjection.class));

        QueryThenFetch leftQtf = (QueryThenFetch) join.left();
        Collect left = (Collect) leftQtf.subPlan();
        assertThat("1 node, otherwise mergePhases would be required", left.nodeIds().size(), is(1));
        assertThat(left.orderBy(), isSQL("OrderByPositions{indices=[1], reverseFlags=[false], nullsFirst=[false]}"));
        assertThat(left.collectPhase().projections(), contains(
            isTopN(10, 2),
            instanceOf(FetchProjection.class)
        ));
        QueryThenFetch rightQtf = (QueryThenFetch) join.right();
        Collect right = (Collect) rightQtf.subPlan();
        assertThat("1 node, otherwise mergePhases would be required", right.nodeIds().size(), is(1));
        assertThat(((RoutedCollectPhase) right.collectPhase()).orderBy(), isSQL("doc.t2.b"));
        assertThat(right.collectPhase().projections(), contains(
            isTopN(5, 5),
            instanceOf(FetchProjection.class),
            instanceOf(EvalProjection.class) // strips `b` used in order by from the outputs
        ));
    }

    @Test
    public void testJoinWithAggregationOnSubSelectsWithLimitAndOffset() throws Exception {
        Join join = e.plan("select t1.a, count(*) from " +
                         " (select i, a from t1 order by a limit 10 offset 2) t1 " +
                         "join" +
                         " (select i from t2 order by i desc limit 5 offset 5) t2 " +
                         "on t1.i = t2.i " +
                         "group by t1.a");

        QueryThenFetch qtf = (QueryThenFetch) join.left();
        Collect left = (Collect) qtf.subPlan();
        assertThat("1 node, otherwise mergePhases would be required", left.nodeIds().size(), is(1));
        assertThat(((RoutedCollectPhase) left.collectPhase()).orderBy(), isSQL("doc.t1.a"));
        assertThat(left.collectPhase().projections(), contains(
            isTopN(10, 2),
            instanceOf(FetchProjection.class)
        ));
        assertThat(left.collectPhase().toCollect(), isSQL("doc.t1._fetchid, doc.t1.a"));


        Collect right = (Collect) join.right();
        assertThat("1 node, otherwise mergePhases would be required", right.nodeIds().size(), is(1));
        assertThat(((RoutedCollectPhase) right.collectPhase()).orderBy(), isSQL("doc.t2.i DESC"));
        assertThat(right.collectPhase().projections(), contains(
            isTopN(5, 5)
        ));


        List<Projection> nlProjections = join.joinPhase().projections();
        assertThat(nlProjections, contains(
            instanceOf(EvalProjection.class),
            instanceOf(GroupProjection.class)
        ));
    }

    @Test
    public void testJoinWithGlobalAggregationOnSubSelectsWithLimitAndOffset() throws Exception {
        Join join = e.plan("select count(*) from " +
                         " (select i, a from t1 order by a limit 10 offset 2) t1 " +
                         "join" +
                         " (select i from t2 order by i desc limit 5 offset 5) t2 " +
                         "on t1.i = t2.i");

        QueryThenFetch leftQtf = (QueryThenFetch) join.left();
        Collect left = (Collect) leftQtf.subPlan();
        assertThat("1 node, otherwise mergePhases would be required", left.nodeIds().size(), is(1));
        assertThat(left.collectPhase().toCollect(), isSQL("doc.t1._fetchid, doc.t1.a"));
        assertThat(((RoutedCollectPhase) left.collectPhase()).orderBy(), isSQL("doc.t1.a"));
        assertThat(left.collectPhase().projections(), contains(
            isTopN(10, 2),
            instanceOf(FetchProjection.class)
        ));

        Collect right = (Collect) join.right();
        assertThat("1 node, otherwise mergePhases would be required", right.nodeIds().size(), is(1));
        assertThat(((RoutedCollectPhase) right.collectPhase()).orderBy(), isSQL("doc.t2.i DESC"));
        assertThat(right.collectPhase().projections(), contains(
            isTopN(5, 5)
        ));

        List<Projection> nlProjections = join.joinPhase().projections();
        assertThat(nlProjections, contains(
            instanceOf(EvalProjection.class),
            instanceOf(AggregationProjection.class)
        ));
    }

    @Test
    public void testJoinWithAggregationOnSubSelectsWithAggregations() throws Exception {
        Join nl = e.plan("select t1.a, count(*) from " +
                         " (select a, count(*) as cnt from t1 group by a) t1 " +
                         "join" +
                         " (select distinct i from t2) t2 " +
                         "on t1.cnt = t2.i::long " +
                         "group by t1.a");
        assertThat(nl.joinPhase().projections(), contains(
            instanceOf(EvalProjection.class),
            instanceOf(GroupProjection.class)
        ));
        assertThat(nl.left(), instanceOf(Collect.class));
        Collect leftPlan = (Collect) nl.left();
        CollectPhase leftCollectPhase = leftPlan.collectPhase();
        assertThat(
            leftCollectPhase.projections(),
            contains(
                instanceOf(GroupProjection.class),
                instanceOf(GroupProjection.class),
                instanceOf(EvalProjection.class)
            )
        );
        Collect rightPlan = (Collect) nl.right();
        assertThat(rightPlan.collectPhase().projections(), contains(
            instanceOf(GroupProjection.class),
            instanceOf(GroupProjection.class)
        ));
    }
}
