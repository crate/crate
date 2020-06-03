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

package io.crate.execution.dsl.projection.builder;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class SplitPointsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable("create table t2 (x int, xs array(integer))")
            .build();
    }

    @Test
    public void testSplitPointsCreationWithFunctionInAggregation() throws Exception {
        QueriedSelectRelation relation = e.analyze("select sum(coalesce(x, 0)) + 10 from t1");

        SplitPoints splitPoints = SplitPointsBuilder.create(relation);

        assertThat(splitPoints.toCollect(), contains(isFunction("coalesce")));
        assertThat(splitPoints.aggregates(), contains(isFunction("sum")));
    }

    @Test
    public void testSplitPointsCreationSelectItemAggregationsAreAlwaysAdded() throws Exception {
        QueriedSelectRelation relation = e.analyze("select" +
                                              "   sum(coalesce(x, 0::integer)), " +
                                              "   sum(coalesce(x, 0::integer)) + 10 " +
                                              "from t1");

        SplitPoints splitPoints = SplitPointsBuilder.create(relation);

        assertThat(splitPoints.toCollect(), contains(isFunction("coalesce")));
        assertThat(splitPoints.aggregates(), contains(isFunction("sum")));
    }


    @Test
    public void testScalarIsNotCollectedEarly() throws Exception {
        QueriedSelectRelation relation = e.analyze("select x + 1 from t1 group by x");

        SplitPoints splitPoints = SplitPointsBuilder.create(relation);
        assertThat(splitPoints.toCollect(), contains(isReference("x")));
        assertThat(splitPoints.aggregates(), Matchers.emptyIterable());
    }

    @Test
    public void testTableFunctionArgsAndStandaloneColumnsAreAddedToCollect() throws Exception {
        QueriedSelectRelation relation = e.analyze("select unnest(xs), x from t2");
        SplitPoints splitPoints = SplitPointsBuilder.create(relation);
        assertThat(splitPoints.toCollect(), contains(isReference("xs"), isReference("x")));
        assertThat(splitPoints.tableFunctions(), contains(isFunction("unnest")));
    }

    @Test
    public void testAggregationPlusTableFunctionUsingAggregation() throws Exception {
        QueriedSelectRelation relation = e.analyze("select max(x), generate_series(0, max(x)) from t1");
        SplitPoints splitPoints = SplitPointsBuilder.create(relation);
        assertThat(splitPoints.toCollect(), contains(isReference("x")));
        assertThat(splitPoints.aggregates(), contains(isFunction("max")));
        assertThat(splitPoints.tableFunctions(), contains(isFunction("generate_series")));
    }

    @Test
    public void test_split_points_creation_with_filter_in_aggregate_expression() {
        QueriedSelectRelation relation = e.analyze("select sum(i) filter (where x > 1) from t1");

        SplitPoints splitPoints = SplitPointsBuilder.create(relation);

        assertThat(splitPoints.toCollect(), contains(
            isReference("i"),
            isFunction("op_>", isReference("x"), isLiteral(1)))
        );
        assertThat(splitPoints.aggregates(), contains(isFunction("sum")));
    }

    @Test
    public void test_split_points_creation_with_filter_in_aggregate_fo_window_function_call() {
        QueriedSelectRelation relation = e.analyze("select sum(i) filter (where x > 1) over(order by i) from t1");

        SplitPoints splitPoints = SplitPointsBuilder.create(relation);

        assertThat(splitPoints.toCollect(), contains(
            isReference("i"),
            isFunction("op_>", isReference("x"), isLiteral(1)))
        );
        assertThat(splitPoints.windowFunctions(), contains(isFunction("sum")));
        assertThat(splitPoints.aggregates(), is(empty()));
    }
}
