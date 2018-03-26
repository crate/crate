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

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

public class PushDownTest extends CrateDummyClusterServiceUnitTest {

    private TableStats tableStats;
    private SQLExecutor sqlExecutor;

    @Before
    public void setup() {
        tableStats = new TableStats();
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addDocTable(T3.T1_INFO)
            .addDocTable(T3.T2_INFO)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
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
        assertThat(plan, isPlan(sqlExecutor.functions(),  "Boundary[name]\n" +
                                                "Union[\n" +
                                                    "Boundary[name]\n" +
                                                    "OrderBy['name' ASC]\n" +
                                                    "Collect[doc.users | [name] | All]\n" +
                                                "---\n" +
                                                    "Boundary[text]\n" +
                                                    "OrderBy['text' ASC]\n" +
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
        assertThat(plan, isPlan(sqlExecutor.functions(),  "Boundary[name]\n" +
                                                          "Union[\n" +
                                                              "Boundary[name]\n" +
                                                              "Boundary[name]\n" +
                                                              "FetchOrEval[name]\n" +
                                                              "OrderBy['name' ASC]\n" +
                                                              "Collect[doc.users | [name, text] | All]\n" +
                                                          "---\n" +
                                                              "Boundary[text]\n" +
                                                              "OrderBy['text' ASC]\n" +
                                                              "Collect[doc.users | [text] | All]\n" +
                                                          "]\n"));
    }

    @Test
    public void testOrderByOnJoinPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t1.a");
        assertThat(plan, isPlan(sqlExecutor.functions(),  "NestedLoopJoin[\n" +
                                                          "    Boundary[a]\n" +
                                                          "    OrderBy['a' ASC]\n" +
                                                          "    Collect[doc.t1 | [a] | All]\n" +
                                                          "    --- INNER ---\n" +
                                                          "    Boundary[b]\n" +
                                                          "    Collect[doc.t2 | [b] | All]\n" +
                                                          "]\n"));
    }

    @Test
    public void testOrderByOnJoinWithMultipleRelationsPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b, t3.a from t1 " +
                                "inner join t2 on t1.a = t2.b " +
                                "inner join t1 as t3 on t3.a = t2.b " +
                                "order by t2.b");
        assertThat(plan, isPlan(sqlExecutor.functions(),  "FetchOrEval[a, b, a]\n" +
                                                          "NestedLoopJoin[\n" +
                                                          "    NestedLoopJoin[\n" +
                                                          "        Boundary[b]\n" +
                                                          "        OrderBy['b' ASC]\n" +
                                                          "        Collect[doc.t2 | [b] | All]\n" +
                                                          "        --- INNER ---\n" +
                                                          "        Boundary[a]\n" +
                                                          "        Collect[doc.t1 | [a] | All]\n" +
                                                          "]\n" +
                                                          "    --- INNER ---\n" +
                                                          "    Boundary[a]\n" +
                                                          "    Collect[doc.t1 | [a] | All]\n" +
                                                          "]\n"));
    }

    @Test
    public void testOrderByOnJoinWithUncollectedColumnPushedDown() {
        LogicalPlan plan = plan("select t2.y, t2.b, t1.i from t1 inner join t2 on t1.a = t2.b order by t1.x desc");
        assertThat(plan, isPlan(sqlExecutor.functions(), "FetchOrEval[y, b, i]\n" +
                                                         "NestedLoopJoin[\n" +
                                                         "    Boundary[_fetchid, a, x]\n" +
                                                         "    FetchOrEval[_fetchid, a, x]\n" +
                                                         "    OrderBy['x' DESC]\n" +
                                                         "    Collect[doc.t1 | [_fetchid, a, x] | All]\n" +
                                                         "    --- INNER ---\n" +
                                                         "    Boundary[_fetchid, b]\n" +
                                                         "    FetchOrEval[_fetchid, b]\n" +
                                                         "    Collect[doc.t2 | [_fetchid, b] | All]\n]" +
                                                         "\n"));
    }

    @Test
    public void testOrderByOnJoinOrderOnRightTableNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t2.b");
        assertThat(plan, isPlan(sqlExecutor.functions(),  "OrderBy['b' ASC]\n" +
                                                          "NestedLoopJoin[\n" +
                                                          "    Boundary[a]\n" +
                                                          "    Collect[doc.t1 | [a] | All]\n" +
                                                          "    --- INNER ---\n" +
                                                          "    Boundary[b]\n" +
                                                          "    Collect[doc.t2 | [b] | All]\n" +
                                                          "]\n"));
    }

    @Test
    public void testOrderByOnJoinOrderOnMultipleTablesNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b from t1 inner join t2 on t1.a = t2.b order by t1.a || t2.b");
        assertThat(plan, isPlan(sqlExecutor.functions(),  "FetchOrEval[a, b]\n" +
                                                          "OrderBy['concat(a, b)' ASC]\n" +
                                                          "NestedLoopJoin[\n" +
                                                          "    Boundary[a]\n" +
                                                          "    Collect[doc.t1 | [a] | All]\n" +
                                                          "    --- INNER ---\n" +
                                                          "    Boundary[b]\n" +
                                                          "    Collect[doc.t2 | [b] | All]\n" +
                                                          "]\n"));
    }

    @Test
    public void testOrderByOnJoinOuterJoinInvolvedNotPushedDown() {
        LogicalPlan plan = plan("select t1.a, t2.b, t3.a from t1 " +
                                "inner join t2 on t1.a = t2.b " +
                                "left join t1 as t3 on t3.a = t1.a " +
                                "order by t1.a");
        assertThat(plan, isPlan(sqlExecutor.functions(),  "FetchOrEval[a, b, a]\n" +
                                                          "OrderBy['a' ASC]\n" +
                                                          "NestedLoopJoin[\n" +
                                                          "    NestedLoopJoin[\n" +
                                                          "        Boundary[b]\n" +
                                                          "        Collect[doc.t2 | [b] | All]\n" +
                                                          "        --- INNER ---\n" +
                                                          "        Boundary[a]\n" +
                                                          "        Collect[doc.t1 | [a] | All]\n" +
                                                          "]\n" +
                                                          "    --- LEFT ---\n" +
                                                          "    Boundary[a]\n" +
                                                          "    Collect[doc.t1 | [a] | All]\n" +
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
        assertThat(plan, isPlan(sqlExecutor.functions(),  "OrderBy['a' ASC]\n" +
                                                          "HashJoin[\n" +
                                                          "    Boundary[a]\n" +
                                                          "    Collect[doc.t1 | [a] | All]\n" +
                                                          "    --- INNER ---\n" +
                                                          "    Boundary[b]\n" +
                                                          "    Collect[doc.t2 | [b] | All]\n" +
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
                "    Boundary[_fetchid, y, b]\n" +
                "    FetchOrEval[_fetchid, y, b]\n" +
                "    Boundary[_fetchid, y, b]\n" +
                "    FetchOrEval[_fetchid, y, b]\n" +
                "    OrderBy['lower(b)' ASC]\n" +
                "    Collect[doc.t2 | [_fetchid, y, b] | All]\n" +
                "    --- INNER ---\n" +
                "    Boundary[_fetchid, x]\n" +
                "    FetchOrEval[_fetchid, x]\n" +
                "    Boundary[_fetchid, x]\n" +
                "    FetchOrEval[_fetchid, x]\n" +
                "    Collect[doc.t1 | [_fetchid, x] | All]\n" +
                "]\n")
        );
    }
}
