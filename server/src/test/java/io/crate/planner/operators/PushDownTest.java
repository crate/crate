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

package io.crate.planner.operators;

import io.crate.analyze.TableDefinitions;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

public class PushDownTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void setup() throws IOException {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        // push down is currently NOT possible when using hash joins. To test the push downs we must disable hash joins.
        sqlExecutor.getSessionContext().setHashJoinEnabled(false);
    }

    private LogicalPlan plan(String stmt) {
        return sqlExecutor.logicalPlan(stmt);
    }

    @Test
    public void testOrderByOnUnionIsMovedBeneathUnion() {
        LogicalPlan plan = plan("Select name from users union all select text from users order by name");
        assertThat(plan, isPlan(
            "Union[name]\n" +
            "  ├ OrderBy[name ASC]\n" +
            "  │  └ Collect[doc.users | [name] | true]\n" +
            "  └ OrderBy[text ASC]\n" +
            "    └ Collect[doc.users | [text] | true]"
        ));
    }

    @Test
    public void testOrderByOnUnionIsCombinedWithOrderByBeneathUnion() {
        LogicalPlan plan = plan(
            "Select * from (select name from users order by text) a " +
            "union all " +
            "select text from users " +
            "order by name");
        assertThat(
            plan,
            isPlan(
                "Union[name]\n" +
                "  ├ Rename[name] AS a\n" +
                "  │  └ Eval[name]\n" +
                "  │    └ OrderBy[name ASC]\n" +
                "  │      └ Collect[doc.users | [name] | true]\n" +
                "  └ OrderBy[text ASC]\n" +
                "    └ Collect[doc.users | [text] | true]"));
    }

    @Test
    public void testOrderByOnJoinPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t1.a");
        assertThat(
            plan,
            isPlan(
                "NestedLoopJoin[INNER | (a = b)]\n" +
                "  ├ OrderBy[a ASC]\n" +
                "  │  └ Collect[doc.t1 | [a] | true]\n" +
                "  └ Collect[doc.t2 | [b] | true]"
            )
        );
    }

    @Test
    public void testOrderByOnJoinWithMultipleRelationsPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b, t3.a from t1 " +
                                "inner join t2 on t1.a = t2.b " +
                                "inner join t1 as t3 on t3.a = t2.b " +
                                "order by t2.b");
        assertThat(plan, isPlan(
            "Eval[a, b, a]\n" +
            "  └ NestedLoopJoin[INNER | (a = b)]\n" +
            "    ├ NestedLoopJoin[INNER | (a = b)]\n" +
            "    │  ├ OrderBy[b ASC]\n" +
            "    │  │  └ Collect[doc.t2 | [b] | true]\n" +
            "    │  └ Collect[doc.t1 | [a] | true]\n" +
            "    └ Rename[a] AS t3\n" +
            "      └ Collect[doc.t1 | [a] | true]"));
    }

    @Test
    public void testOrderByOnJoinWithUncollectedColumnPushedDown() {
        LogicalPlan plan = plan("select t2.y, t2.b, t1.i from t1 inner join t2 on t1.a = t2.b order by t1.x desc");
        assertThat(plan, isPlan(
            "Eval[y, b, i]\n" +
            "  └ NestedLoopJoin[INNER | (a = b)]\n" +
            "    ├ OrderBy[x DESC]\n" +
            "    │  └ Collect[doc.t1 | [i, x, a] | true]\n" +
            "    └ Collect[doc.t2 | [y, b] | true]"));
    }

    @Test
    public void testOrderByOnJoinOrderOnRightTableNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t2.b");
        assertThat(
            plan,
            isPlan(
                "OrderBy[b ASC]\n" +
                "  └ NestedLoopJoin[INNER | (a = b)]\n" +
                "    ├ Collect[doc.t1 | [a] | true]\n" +
                "    └ Collect[doc.t2 | [b] | true]"
            )
        );
    }

    @Test
    public void testOrderByOnJoinOrderOnMultipleTablesNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t1.a || t2.b");
        assertThat(
            plan,
            isPlan(
                "Eval[a, b]\n" +
                "  └ OrderBy[concat(a, b) ASC]\n" +
                "    └ NestedLoopJoin[INNER | (a = b)]\n" +
                "      ├ Collect[doc.t1 | [a] | true]\n" +
                "      └ Collect[doc.t2 | [b] | true]"
            )
        );
    }

    @Test
    public void testFilterIsMovedBeneathOrder() {
        LogicalPlan plan = plan("select * from (select * from t1 order by a) tt where a > '10'");
        assertThat(plan, isPlan(
            "Rename[a, x, i] AS tt\n" +
            "  └ OrderBy[a ASC]\n" +
            "    └ Collect[doc.t1 | [a, x, i] | (a > '10')]"));
    }

    @Test
    public void testOrderByOnJoinOuterJoinInvolvedNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b, t3.a from t1 " +
                                "inner join t2 on t1.a = t2.b " +
                                "left join t1 as t3 on t3.a = t1.a " +
                                "order by t1.a");
        assertThat(plan, isPlan(
            "Eval[a, b, a]\n" +
            "  └ OrderBy[a ASC]\n" +
            "    └ NestedLoopJoin[LEFT | (a = a)]\n" +
            "      ├ NestedLoopJoin[INNER | (a = b)]\n" +
            "      │  ├ Collect[doc.t2 | [b] | true]\n" +
            "      │  └ Collect[doc.t1 | [a] | true]\n" +
            "      └ Rename[a] AS t3\n" +
            "        └ Collect[doc.t1 | [a] | true]"));
    }

    @Test
    public void testOrderByWithHashJoinNotPushedDown() {
        sqlExecutor.getSessionContext().setHashJoinEnabled(true);
        LogicalPlan plan = sqlExecutor.logicalPlan("select t1.a, t2.b " +
                                                   "from t1 inner join t2 on t1.a = t2.b " +
                                                   "order by t1.a");
        sqlExecutor.getSessionContext().setHashJoinEnabled(false);
        assertThat(plan, isPlan(
            "OrderBy[a ASC]\n" +
            "  └ HashJoin[(a = b)]\n" +
            "    ├ Collect[doc.t1 | [a] | true]\n" +
            "    └ Collect[doc.t2 | [b] | true]"
        ));
    }

    @Test
    public void testOrderByIsPushedDownToLeftSide() {
        sqlExecutor.getSessionContext().setHashJoinEnabled(false);
        // differs from testOrderByOnJoinPushedDown in that here the ORDER BY expression is not part of the outputs
        LogicalPlan plan = sqlExecutor.logicalPlan(
            "SELECT t1.i, t2.i FROM t2 INNER JOIN t1 ON t1.x = t2.y ORDER BY lower(t2.b)");

        assertThat(
            plan,
            LogicalPlannerTest.isPlan(
                "Eval[i, i]\n" +
                "  └ NestedLoopJoin[INNER | (x = y)]\n" +
                "    ├ OrderBy[lower(b) ASC]\n" +
                "    │  └ Collect[doc.t2 | [i, b, y] | true]\n" +
                "    └ Collect[doc.t1 | [i, x] | true]"
            )
        );
    }

    @Test
    public void testWhereClauseIsPushedDownIntoSubQuery() {
        LogicalPlan plan = sqlExecutor.logicalPlan(
            "SELECT name FROM (SELECT id, name FROM sys.nodes) t " +
            "WHERE id = 'nodeName'");
        assertThat(
            plan,
            LogicalPlannerTest.isPlan(
                "Eval[name]\n" +
                "  └ Rename[name] AS t\n" +
                "    └ Collect[sys.nodes | [name] | (id = 'nodeName')]"
            )
        );
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsPushedBeneathJoin() {
        sqlExecutor.getSessionContext().setHashJoinEnabled(true);
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM " +
            "   (SELECT * FROM t1 INNER JOIN t2 on t1.x = t2.y) tjoin " +
            "WHERE tjoin.x = 10 "
        );
        var expectedPlan =
            "Rename[a, x, i, b, y, i] AS tjoin\n" +
            "  └ HashJoin[(x = y)]\n" +
            "    ├ Collect[doc.t1 | [a, x, i] | (x = 10)]\n" +
            "    └ Collect[doc.t2 | [b, y, i] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsPushedBeneathNestedLoopJoin() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM " +
            "   (SELECT * FROM t1, t2) tjoin " +
            "WHERE tjoin.x = 10 "
        );
        var expectedPlan =
            "Rename[a, x, i, b, y, i] AS tjoin\n" +
            "  └ NestedLoopJoin[CROSS]\n" +
            "    ├ Collect[doc.t1 | [a, x, i] | (x = 10)]\n" +
            "    └ Collect[doc.t2 | [b, y, i] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsSplitAndPartiallyPushedBeneathJoin() {
        sqlExecutor.getSessionContext().setHashJoinEnabled(true);
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM " +
            "   (SELECT * FROM t1 INNER JOIN t2 on t1.x = t2.y) tjoin " +
            "WHERE tjoin.x = 10 AND tjoin.y = 20 AND (tjoin.a || tjoin.b = '')"
        );
        var expectedPlan =
            "Rename[a, x, i, b, y, i] AS tjoin\n" +
            "  └ Filter[(concat(a, b) = '')]\n" +
            "    └ HashJoin[(x = y)]\n" +
            "      ├ Collect[doc.t1 | [a, x, i] | (x = 10)]\n" +
            "      └ Collect[doc.t2 | [b, y, i] | (y = 20)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testWhereClauseIsPushedDownIntoSubRelationOfUnion() {
        LogicalPlan plan = sqlExecutor.logicalPlan("SELECT * FROM (" +
                                                   "    SELECT name FROM sys.nodes " +
                                                   "    WHERE name like 'b%' " +
                                                   "    UNION ALL " +
                                                   "    SELECT substr(name, 0, 1) as n from sys.cluster " +
                                                   "    WHERE name like 'a%' ) u " +
                                                   "WHERE u.name like 'c%' ");
        assertThat(
            plan,
            LogicalPlannerTest.isPlan(
                "Rename[name] AS u\n" +
                "  └ Union[name]\n" +
                "    ├ Collect[sys.nodes | [name] | ((name LIKE 'c%') AND (name LIKE 'b%'))]\n" +
                "    └ Collect[sys.cluster | [substr(name, 0, 1) AS n] | ((substr(name, 0, 1) AS n LIKE 'c%') AND (name LIKE 'a%'))]"
            )
        );
    }

    @Test
    public void test_filters_on_project_set_are_pushed_down_for_standalone_outputs() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT" +
            "  * " +
            "FROM " +
            "  (SELECT x, generate_series(1::int, x) FROM t1) tt " +
            "WHERE x > 1"
        );
        var expectedPlan =
            "Rename[x, \"generate_series(1, x)\"] AS tt\n" +
            "  └ Eval[x, generate_series(1, x)]\n" +
            "    └ ProjectSet[generate_series(1, x), x]\n" +
            "      └ Collect[doc.t1 | [x] | (x > 1)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_filters_on_project_set_are_pushed_down_only_for_standalone_outputs() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT" +
            "  * " +
            "FROM " +
            "  (SELECT x, generate_series(1::int, x) AS y FROM t1) tt " +
            "WHERE x > 1 AND y > 2"
        );
        var expectedPlan =
            "Rename[x, y] AS tt\n" +
            "  └ Eval[x, generate_series(1, x) AS y]\n" +
            "    └ Filter[(generate_series(1, x) AS y > 2)]\n" +
            "      └ ProjectSet[generate_series(1, x), x]\n" +
            "        └ Collect[doc.t1 | [x] | (x > 1)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterIsPushedBeneathGroupIfOnGroupKeys() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT x, count(*) FROM t1 GROUP BY 1 HAVING x > 1"
        );
        var expectedPlan =
            "GroupHashAggregate[x | count(*)]\n" +
            "  └ Collect[doc.t1 | [x] | (x > 1)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnGroupKeyAndOnAggregateIsPartiallyPushedBeneathGroupBy() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT x, count(*) FROM t1 GROUP BY 1 HAVING count(*) > 10 AND x > 1"
        );
        var expectedPlan =
            "Filter[(count(*) > 10::bigint)]\n" +
            "  └ GroupHashAggregate[x | count(*)]\n" +
            "    └ Collect[doc.t1 | [x] | (x > 1)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnSubQueryPartitionKeyIsPushedBeneathPartitionedWindowAgg() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM " +
            "   (SELECT x, sum(x) over (partition by x) FROM t1) sums " +
            "WHERE sums.x = 10 "
        );
        var expectedPlan =
            "Rename[x, \"sum(x) OVER (PARTITION BY x)\"] AS sums\n" +
            "  └ WindowAgg[x, sum(x) OVER (PARTITION BY x)]\n" +
            "    └ Collect[doc.t1 | [x] | (x = 10)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_count_start_aggregate_filter_is_pushed_down() {
        var plan = plan("SELECT COUNT(*) FILTER (WHERE x > 1) FROM t1");
        var expectedPlan = "Count[doc.t1 | (x > 1)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_count_start_aggregate_filter_is_merged_with_where_clause_query() {
        var plan = plan(
            "SELECT COUNT(*) FILTER (WHERE x > 1) " +
            "FROM t1 " +
            "WHERE x > 10");
        var expectedPlan = "Count[doc.t1 | ((x > 10) AND (x > 1))]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void test_filter_on_pk_column_on_derived_table_is_optimized_to_get() {
        // the ORDER BY id, name is here to avoid a collect-then-fetch, which would (currently) break the Get optimization
        var plan = plan(
            "SELECT id, name FROM (SELECT id, name FROM users ORDER BY id, name) AS u WHERE id = 1 ORDER BY 1, 2");
        var expectedPlan =
            "Rename[id, name] AS u\n" +
            "  └ OrderBy[id ASC name ASC]\n" +
            "    └ OrderBy[id ASC name ASC]\n" +
            "      └ Get[doc.users | id, name | DocKeys{1::bigint}]";
        assertThat(plan, isPlan(expectedPlan));
    }
}
