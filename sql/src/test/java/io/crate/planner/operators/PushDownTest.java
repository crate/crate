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
import io.crate.planner.TableStats;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

public class PushDownTest extends CrateDummyClusterServiceUnitTest {

    private TableStats tableStats;
    private SQLExecutor sqlExecutor;

    @Before
    public void setup() throws IOException {
        tableStats = new TableStats();
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .build();

        // push down is currently NOT possible when using hash joins. To test the push downs we must disable hash joins.
        sqlExecutor.getSessionContext().setHashJoinEnabled(false);
    }

    private LogicalPlan plan(String stmt) {
        return LogicalPlannerTest.plan(stmt, sqlExecutor, clusterService, tableStats);
    }

    @Test
    public void testOrderByOnUnionIsMovedBeneathUnion() {
        LogicalPlan plan = plan("Select name from users union all select text from users order by name");
        assertThat(plan, isPlan(sqlExecutor.functions(), "Boundary[name]\n" +
                                                "Union[\n" +
                                                    "OrderBy[name ASC]\n" +
                                                    "Collect[doc.users | [name] | All]\n" +
                                                "---\n" +
                                                    "OrderBy[text ASC]\n" +
                                                    "Collect[doc.users | [text] | All]\n" +
                                                "]\n"));
    }

    @Test
    public void testOrderByOnUnionIsCombinedWithOrderByBeneathUnion() {
        LogicalPlan plan = plan(
            "Select * from (select name from users order by text) a " +
            "union all " +
            "select text from users " +
            "order by name");
        assertThat(plan, isPlan(sqlExecutor.functions(), "Boundary[name]\n" +
                                                         "Union[\n" +
                                                             "Boundary[name]\n" +
                                                             "FetchOrEval[name]\n" +
                                                             "OrderBy[name ASC]\n" +
                                                             "Collect[doc.users | [name, text] | All]\n" +
                                                         "---\n" +
                                                             "OrderBy[text ASC]\n" +
                                                             "Collect[doc.users | [text] | All]\n" +
                                                         "]\n"));
    }

    @Test
    public void testOrderByOnJoinPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t1.a");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[a, b]\n" +
                                                         "NestedLoopJoin[\n" +
                                                         "    Boundary[_fetchid, a]\n" +
                                                         "    OrderBy[a ASC]\n" +
                                                         "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                         "    --- INNER ---\n" +
                                                         "    Boundary[_fetchid, b]\n" +
                                                         "    Collect[doc.t2 | [_fetchid, b] | All]\n" +
                                                         "]\n"));
    }

    @Test
    public void testOrderByOnJoinWithMultipleRelationsPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b, t3.a from t1 " +
                                "inner join t2 on t1.a = t2.b " +
                                "inner join t1 as t3 on t3.a = t2.b " +
                                "order by t2.b");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[a, b, a]\n" +
                                                         "NestedLoopJoin[\n" +
                                                         "    NestedLoopJoin[\n" +
                                                         "        Boundary[_fetchid, b]\n" +
                                                         "        OrderBy[b ASC]\n" +
                                                         "        Collect[doc.t2 | [_fetchid, b] | All]\n" +
                                                         "        --- INNER ---\n" +
                                                         "        Boundary[_fetchid, a]\n" +
                                                         "        Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                         "]\n" +
                                                         "    --- INNER ---\n" +
                                                         "    Boundary[_fetchid, a]\n" +
                                                         "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                         "]\n"));
    }

    @Test
    public void testOrderByOnJoinWithUncollectedColumnPushedDown() {
        LogicalPlan plan = plan("select t2.y, t2.b, t1.i from t1 inner join t2 on t1.a = t2.b order by t1.x desc");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[y, b, i]\n" +
                                                         "NestedLoopJoin[\n" +
                                                         "    Boundary[_fetchid, a, x]\n" +
                                                         "    OrderBy[x DESC]\n" +
                                                         "    Collect[doc.t1 | [_fetchid, a, x] | All]\n" +
                                                         "    --- INNER ---\n" +
                                                         "    Boundary[_fetchid, b]\n" +
                                                         "    Collect[doc.t2 | [_fetchid, b] | All]\n]" +
                                                         "\n"));
    }

    @Test
    public void testOrderByOnJoinOrderOnRightTableNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t2.b");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[a, b]\n" +
                                                         "OrderBy[b ASC]\n" +
                                                         "NestedLoopJoin[\n" +
                                                         "    Boundary[_fetchid, a]\n" +
                                                         "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                         "    --- INNER ---\n" +
                                                         "    Boundary[_fetchid, b]\n" +
                                                         "    Collect[doc.t2 | [_fetchid, b] | All]\n" +
                                                         "]\n"));
    }

    @Test
    public void testOrderByOnJoinOrderOnMultipleTablesNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t1.a || t2.b");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[a, b]\n" +
                                                          "OrderBy[concat(a, b) ASC]\n" +
                                                          "NestedLoopJoin[\n" +
                                                          "    Boundary[_fetchid, a]\n" +
                                                          "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                          "    --- INNER ---\n" +
                                                          "    Boundary[_fetchid, b]\n" +
                                                          "    Collect[doc.t2 | [_fetchid, b] | All]\n" +
                                                          "]\n"));
    }

    @Test
    public void testFilterIsMovedBeneathOrder() {
        LogicalPlan plan = plan("select * from (select * from t1 order by a) tt where a > 10");
        assertThat(plan, isPlan(sqlExecutor.functions(),
            "FetchOrEval[a, x, i]\n" +
            "Boundary[_fetchid, a]\n" +
            "OrderBy[a ASC]\n" +
            "Collect[doc.t1 | [_fetchid, a] | (a > '10')]\n"
        ));
    }

    @Test
    public void testOrderByOnJoinOuterJoinInvolvedNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b, t3.a from t1 " +
                                "inner join t2 on t1.a = t2.b " +
                                "left join t1 as t3 on t3.a = t1.a " +
                                "order by t1.a");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[a, b, a]\n" +
                                                         "OrderBy[a ASC]\n" +
                                                         "NestedLoopJoin[\n" +
                                                         "    NestedLoopJoin[\n" +
                                                         "        Boundary[_fetchid, b]\n" +
                                                         "        Collect[doc.t2 | [_fetchid, b] | All]\n" +
                                                         "        --- INNER ---\n" +
                                                         "        Boundary[_fetchid, a]\n" +
                                                         "        Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                         "]\n" +
                                                         "    --- LEFT ---\n" +
                                                         "    Boundary[_fetchid, a]\n" +
                                                         "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
                                                         "]\n"));
    }

    @Test
    public void testOrderByWithHashJoinNotPushedDown() {
        sqlExecutor.getSessionContext().setHashJoinEnabled(true);
        LogicalPlan plan = LogicalPlannerTest.plan("select t1.a, t2.b " +
                                                   "from t1 inner join t2 on t1.a = t2.b " +
                                                   "order by t1.a",
            sqlExecutor, clusterService, tableStats);
        sqlExecutor.getSessionContext().setHashJoinEnabled(false);
        assertThat(plan, isPlan(sqlExecutor.functions(),
            "FetchOrEval[a, b]\n" +
            "OrderBy[a ASC]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, a]\n" +
            "    Collect[doc.t1 | [_fetchid, a] | All]\n" +
            "    --- INNER ---\n" +
            "    Boundary[_fetchid, b]\n" +
            "    Collect[doc.t2 | [_fetchid, b] | All]\n" +
            "]\n"));
    }

    @Test
    public void testOrderByIsPushedDownToLeftSide() {
        sqlExecutor.getSessionContext().setHashJoinEnabled(false);
        // differs from testOrderByOnJoinPushedDown in that here the ORDER BY expression is not part of the outputs
        LogicalPlan plan = sqlExecutor.logicalPlan(
            "SELECT t1.i, t2.i FROM t2 INNER JOIN t1 ON t1.x = t2.y ORDER BY lower(t2.b)");

        assertThat(
            plan,
            LogicalPlannerTest.isPlan(sqlExecutor.functions(),
                "RootBoundary[i, i]\n" +
                "FetchOrEval[i, i]\n" +
                "NestedLoopJoin[\n" +
                "    Boundary[_fetchid, b, y]\n" +
                "    FetchOrEval[_fetchid, b, y]\n" +
                "    OrderBy[lower(b) ASC]\n" +
                "    Collect[doc.t2 | [_fetchid, y, b] | All]\n" +
                "    --- INNER ---\n" +
                "    Boundary[_fetchid, x]\n" +
                "    Collect[doc.t1 | [_fetchid, x] | All]\n" +
                "]\n")
        );
    }

    @Test
    public void testWhereClauseIsPushedDownIntoSubQuery() {
        LogicalPlan plan = sqlExecutor.logicalPlan("SELECT name FROM (SELECT id, name FROM sys.nodes) t " +
                                                   "WHERE id = 'nodeName'");

        assertThat(
            plan,
            LogicalPlannerTest.isPlan(sqlExecutor.functions(),
                "RootBoundary[name]\n" +
                "FetchOrEval[name]\n" +
                "Boundary[id, name]\n" +
                "Collect[sys.nodes | [id, name] | (id = 'nodeName')]\n"));
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
            "RootBoundary[a, x, i, b, y, i]\n" +
            "FetchOrEval[a, x, i, b, y, i]\n" +
            "Boundary[_fetchid, _fetchid, x, y]\n" +
            "FetchOrEval[_fetchid, _fetchid, x, y]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, x]\n" +
            "    Collect[doc.t1 | [_fetchid, x] | (x = 10)]\n" +
            "    --- INNER ---\n" +
            "    Boundary[_fetchid, y]\n" +
            "    Collect[doc.t2 | [_fetchid, y] | All]\n" +
            "]\n";
        assertThat(plan, isPlan(sqlExecutor.functions(), expectedPlan));
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsPushedBeneathNestedLoopJoin() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM " +
            "   (SELECT * FROM t1, t2) tjoin " +
            "WHERE tjoin.x = 10 "
        );
        var expectedPlan =
            "RootBoundary[a, x, i, b, y, i]\n" +
            "FetchOrEval[a, x, i, b, y, i]\n" +
            "Boundary[_fetchid, _fetchid, x]\n" +
            "FetchOrEval[_fetchid, _fetchid, x]\n" +
            "NestedLoopJoin[\n" +
            "    Boundary[_fetchid, x]\n" +
            "    Collect[doc.t1 | [_fetchid, x] | (x = 10)]\n" +
            "    --- CROSS ---\n" +
            "    Boundary[_fetchid]\n" +
            "    Collect[doc.t2 | [_fetchid] | All]\n" +
            "]\n";
        assertThat(plan, isPlan(sqlExecutor.functions(), expectedPlan));
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
            "RootBoundary[a, x, i, b, y, i]\n" +
            "FetchOrEval[a, x, i, b, y, i]\n" +
            "Boundary[_fetchid, _fetchid, a, x, b, y]\n" +
            "FetchOrEval[_fetchid, _fetchid, a, x, b, y]\n" +
            "Filter[(concat(a, b) = '')]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, a, x]\n" +
            "    FetchOrEval[_fetchid, a, x]\n" +
            "    Collect[doc.t1 | [_fetchid, x, a] | (x = 10)]\n" +
            "    --- INNER ---\n" +
            "    Boundary[_fetchid, b, y]\n" +
            "    FetchOrEval[_fetchid, b, y]\n" +
            "    Collect[doc.t2 | [_fetchid, y, b] | (y = 20)]\n" +
            "]\n";
        assertThat(plan, isPlan(sqlExecutor.functions(), expectedPlan));
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
            LogicalPlannerTest.isPlan(sqlExecutor.functions(),
                "RootBoundary[name]\n" +
                "Boundary[name]\n" +
                "Union[\n" +
                "Collect[sys.nodes | [name] | ((name LIKE 'b%') AND (name LIKE 'c%'))]\n" +
                "---\n" +
                "Collect[sys.cluster | [substr(name, 0, 1)] | ((name LIKE 'a%') AND (substr(name, 0, 1) LIKE 'c%'))]\n]\n"
            )
        );
    }

    @Test
    public void testFilterIsPushedBeneathGroupIfOnGroupKeys() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT x, count(*) FROM t1 GROUP BY 1 HAVING x > 1"
        );
        var expectedPlan =
            "RootBoundary[x, count(*)]\n" +
            "GroupBy[x | count(*)]\n" +
            "Collect[doc.t1 | [x] | (x > 1)]\n";
        assertThat(plan, isPlan(sqlExecutor.functions(), expectedPlan));
    }

    @Test
    public void testFilterOnGroupKeyAndOnAggregateIsPartiallyPushedBeneathGroupBy() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT x, count(*) FROM t1 GROUP BY 1 HAVING count(*) > 10 AND x > 1"
        );
        var expectedPlan =
            "RootBoundary[x, count(*)]\n" +
            "Filter[(count(*) > 10)]\n" +
            "GroupBy[x | count(*)]\n" +
            "Collect[doc.t1 | [x] | (x > 1)]\n";
        assertThat(plan, isPlan(sqlExecutor.functions(), expectedPlan));
    }
}
