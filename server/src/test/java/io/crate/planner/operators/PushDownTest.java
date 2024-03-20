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
 * distributed under the License is distributed ON an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.operators;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.planner.node.dql.CountPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class PushDownTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void setup() throws IOException {
        sqlExecutor = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);

        // push down is currently NOT possible when using hash joins. To test the push downs we must disable hash joins.
        sqlExecutor.getSessionSettings().setHashJoinEnabled(false);
    }

    private LogicalPlan plan(String stmt) {
        return sqlExecutor.logicalPlan(stmt);
    }

    @Test
    public void testOrderByOnUnionIsMovedBeneathUnion() {
        LogicalPlan plan = plan("SELECT name FROM users UNION ALL SELECT text FROM users ORDER BY name");
        assertThat(plan).isEqualTo(
            """
            Union[name]
              ├ OrderBy[name ASC]
              │  └ Collect[doc.users | [name] | true]
              └ OrderBy[text ASC]
                └ Collect[doc.users | [text] | true]
            """
        );
    }

    @Test
    public void testOrderByOnUnionIsCombinedWithOrderByBeneathUnion() {
        LogicalPlan plan = plan(
            """
            SELECT * FROM (SELECT name FROM users ORDER BY text) a
            UNION ALL
            SELECT text FROM users
            ORDER BY name
            """);
        assertThat(plan).isEqualTo(
            """
            Union[name]
              ├ Rename[name] AS a
              │  └ OrderBy[name ASC]
              │    └ Collect[doc.users | [name] | true]
              └ OrderBy[text ASC]
                └ Collect[doc.users | [text] | true]
            """
        );
    }

    @Test
    public void testOrderByOnJoinPushedDown() {
        LogicalPlan plan = plan("SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.b ORDER BY t1.a");
        assertThat(plan).isEqualTo(
            """
            NestedLoopJoin[INNER | (a = b)]
              ├ OrderBy[a ASC]
              │  └ Collect[doc.t1 | [a] | true]
              └ Collect[doc.t2 | [b] | true]
            """
        );
    }

    @Test
    public void testOrderByOnJoinWithMultipleRelationsPushedDown() {
        LogicalPlan plan = plan(
            """
            SELECT t1.a, t2.b, t3.a FROM t1
            INNER JOIN t2 ON t1.a = t2.b
            INNER JOIN t1 as t3 ON t3.a = t2.b
            ORDER BY t1.a
            """);
        assertThat(plan).isEqualTo(
            """
            NestedLoopJoin[INNER | (a = b)]
              ├ NestedLoopJoin[INNER | (a = b)]
              │  ├ OrderBy[a ASC]
              │  │  └ Collect[doc.t1 | [a] | true]
              │  └ Collect[doc.t2 | [b] | true]
              └ Rename[a] AS t3
                └ Collect[doc.t1 | [a] | true]
             """
        );
    }

    @Test
    public void testOrderByOnJoinWithUncollectedColumnPushedDown() {
        LogicalPlan plan = plan("SELECT t2.y, t2.b, t1.i FROM t1 INNER JOIN t2 ON t1.a = t2.b ORDER BY t1.x desc");
        assertThat(plan).isEqualTo(
            """
            Eval[y, b, i]
              └ NestedLoopJoin[INNER | (a = b)]
                ├ OrderBy[x DESC]
                │  └ Collect[doc.t1 | [i, x, a] | true]
                └ Collect[doc.t2 | [y, b] | true]
            """);
    }

    @Test
    public void testOrderByOnJoinOrderOnRightTableNotPushedDown() {
        LogicalPlan plan = plan("SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.b ORDER BY t2.b");
        assertThat(plan).isEqualTo(
            """
            OrderBy[b ASC]
              └ NestedLoopJoin[INNER | (a = b)]
                ├ Collect[doc.t1 | [a] | true]
                └ Collect[doc.t2 | [b] | true]
            """
        );
    }

    @Test
    public void testOrderByOnJoinOrderOnMultipleTablesNotPushedDown() {
        LogicalPlan plan = plan("SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.b ORDER BY t1.a || t2.b");
        assertThat(plan).isEqualTo(
            """
            Eval[a, b]
              └ OrderBy[concat(a, b) ASC]
                └ NestedLoopJoin[INNER | (a = b)]
                  ├ Collect[doc.t1 | [a] | true]
                  └ Collect[doc.t2 | [b] | true]
            """
        );
    }

    @Test
    public void testFilterIsMovedBeneathOrder() {
        LogicalPlan plan = plan("SELECT * FROM (SELECT * FROM t1 ORDER BY a) tt where a > '10'");
        assertThat(plan).isEqualTo(
            """
            Fetch[a, x, i]
              └ Rename[tt._fetchid, a] AS tt
                └ OrderBy[a ASC]
                  └ Collect[doc.t1 | [_fetchid, a] | (a > '10')]
            """
        );
    }

    @Test
    public void testOrderByOnJoinOuterJoinInvolvedNotPushedDown() {
        LogicalPlan plan = plan(
            """
            SELECT t1.a, t2.b, t3.a FROM t1
            INNER JOIN t2 ON t1.a = t2.b
            LEFT JOIN t1 as t3 ON t3.a = t1.a
            ORDER BY t1.a
            """
        );
        assertThat(plan).isEqualTo(
            """
            OrderBy[a ASC]
              └ NestedLoopJoin[LEFT | (a = a)]
                ├ NestedLoopJoin[INNER | (a = b)]
                │  ├ Collect[doc.t1 | [a] | true]
                │  └ Collect[doc.t2 | [b] | true]
                └ Rename[a] AS t3
                  └ Collect[doc.t1 | [a] | true]
            """
        );
    }

    @Test
    public void testOrderByWithHashJoinNotPushedDown() {
        sqlExecutor.getSessionSettings().setHashJoinEnabled(true);
        LogicalPlan plan = sqlExecutor.logicalPlan(
            """
            SELECT t1.a, t2.b
            FROM t1 INNER JOIN t2 ON t1.a = t2.b
            ORDER BY t1.a
            """);
        sqlExecutor.getSessionSettings().setHashJoinEnabled(false);
        assertThat(plan).isEqualTo(
            """
            OrderBy[a ASC]
              └ HashJoin[(a = b)]
                ├ Collect[doc.t1 | [a] | true]
                └ Collect[doc.t2 | [b] | true]
            """
        );
    }

    @Test
    public void testOrderByIsPushedDownToLeftSide() {
        sqlExecutor.getSessionSettings().setHashJoinEnabled(false);
        // differs FROM testOrderByOnJoinPushedDown in that here the ORDER BY expression is not part of the outputs
        LogicalPlan plan = sqlExecutor.logicalPlan(
            "SELECT t1.i, t2.i FROM t2 INNER JOIN t1 ON t1.x = t2.y ORDER BY lower(t2.b)");

        assertThat(plan).isEqualTo(
            """
            Eval[i, i]
              └ NestedLoopJoin[INNER | (x = y)]
                ├ OrderBy[lower(b) ASC]
                │  └ Collect[doc.t2 | [i, b, y] | true]
                └ Collect[doc.t1 | [i, x] | true]
            """
        );
    }

    @Test
    public void testWhereClauseIsPushedDownIntoSubQuery() {
        LogicalPlan plan = sqlExecutor.logicalPlan(
            "SELECT name FROM (SELECT id, name FROM sys.nodes) t " +
            "WHERE id = 'nodeName'");
        assertThat(plan).isEqualTo(
            """
            Rename[name] AS t
              └ Collect[sys.nodes | [name] | (id = 'nodeName')]
            """
        );
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsPushedBeneathJoin() {
        sqlExecutor.getSessionSettings().setHashJoinEnabled(true);
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT * FROM
              (SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y) tjoin
            WHERE tjoin.x = 10
            """
        );
        var expectedPlan =
            """
            Rename[a, x, i, b, y, i] AS tjoin
              └ HashJoin[(x = y)]
                ├ Collect[doc.t1 | [a, x, i] | (x = 10)]
                └ Collect[doc.t2 | [b, y, i] | true]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsPushedBeneathNestedLoopJoin() {
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT * FROM
              (SELECT * FROM t1, t2) tjoin
            WHERE tjoin.x = 10
            """
        );
        var expectedPlan =
            """
            Rename[a, x, i, b, y, i] AS tjoin
              └ NestedLoopJoin[CROSS]
                ├ Collect[doc.t1 | [a, x, i] | (x = 10)]
                └ Collect[doc.t2 | [b, y, i] | true]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testFilterOnSubQueryWithJoinIsSplitAndPartiallyPushedBeneathJoin() {
        sqlExecutor.getSessionSettings().setHashJoinEnabled(true);
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT * FROM
              (SELECT * FROM t1 INNER JOIN t2 ON t1.x = t2.y) tjoin
            WHERE tjoin.x = 10 AND tjoin.y = 20 AND (tjoin.a || tjoin.b = '')
            """
        );
        var expectedPlan =
            """
            Rename[a, x, i, b, y, i] AS tjoin
              └ Filter[(concat(a, b) = '')]
                └ HashJoin[(x = y)]
                  ├ Collect[doc.t1 | [a, x, i] | (x = 10)]
                  └ Collect[doc.t2 | [b, y, i] | (y = 20)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testWhereClauseIsPushedDownIntoSubRelationOfUnion() {
        LogicalPlan plan = sqlExecutor.logicalPlan(
            """
            SELECT * FROM (
              SELECT name FROM sys.nodes
              WHERE name like 'b%'
              UNION ALL
              SELECT substr(name, 0, 1) as n FROM sys.cluster
              WHERE name like 'a%' ) u
            WHERE u.name like 'c%'
            """
        );
        assertThat(plan).isEqualTo(
            """
            Rename[name] AS u
              └ Union[name]
                ├ Collect[sys.nodes | [name] | ((name LIKE 'c%') AND (name LIKE 'b%'))]
                └ Collect[sys.cluster | [substr(name, 0, 1) AS n] | ((substr(name, 0, 1) AS n LIKE 'c%') AND (name LIKE 'a%'))]
            """
        );
    }

    @Test
    public void test_filters_on_project_set_are_pushed_down_for_standalone_outputs() {
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT * FROM
              (SELECT x, generate_series(1::int, x) FROM t1) tt
            WHERE x > 1
            """
        );
        var expectedPlan =
            """
            Rename[x, "pg_catalog.generate_series(1, x)"] AS tt
              └ Eval[x, pg_catalog.generate_series(1, x)]
                └ ProjectSet[pg_catalog.generate_series(1, x), x]
                  └ Collect[doc.t1 | [x] | (x > 1)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_filters_on_project_set_are_pushed_down_only_for_standalone_outputs() {
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT *
            FROM
              (SELECT x, generate_series(1::int, x) AS y FROM t1) tt
            WHERE x > 1 AND y > 2
            """
        );
        var expectedPlan =
            """
            Rename[x, y] AS tt
              └ Eval[x, pg_catalog.generate_series(1, x) AS y]
                └ Filter[(pg_catalog.generate_series(1, x) AS y > 2)]
                  └ ProjectSet[pg_catalog.generate_series(1, x), x]
                    └ Collect[doc.t1 | [x] | (x > 1)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testFilterIsPushedBeneathGroupIfOnGroupKeys() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT x, count(*) FROM t1 GROUP BY 1 HAVING x > 1"
        );
        var expectedPlan =
            """
            GroupHashAggregate[x | count(*)]
              └ Collect[doc.t1 | [x] | (x > 1)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testFilterOnGroupKeyAndOnAggregateIsPartiallyPushedBeneathGroupBy() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT x, count(*) FROM t1 GROUP BY 1 HAVING count(*) > 10 AND x > 1"
        );
        var expectedPlan =
            """
            Filter[(count(*) > 10::bigint)]
              └ GroupHashAggregate[x | count(*)]
                └ Collect[doc.t1 | [x] | (x > 1)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void testFilterOnSubQueryPartitionKeyIsPushedBeneathPartitionedWindowAgg() {
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT * FROM
              (SELECT x, sum(x) over (partition by x) FROM t1) sums
            WHERE sums.x = 10
            """
        );
        var expectedPlan =
            """
            Rename[x, "sum(x) OVER (PARTITION BY x)"] AS sums
              └ WindowAgg[x, sum(x) OVER (PARTITION BY x)]
                └ Collect[doc.t1 | [x] | (x = 10)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_count_start_aggregate_filter_is_pushed_down() {
        var plan = plan("SELECT COUNT(*) FILTER (WHERE x > 1) FROM t1");
        var expectedPlan = "Count[doc.t1 | (x > 1)]";
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_count_start_aggregate_filter_on_aliased_table_is_pushed_down() {
        String stmt = "SELECT COUNT(*) FILTER (WHERE x > 1) FROM t1 as t";
        var plan = plan(stmt);
        var expectedPlan = "Count[doc.t1 | (x > 1)]";
        assertThat(plan).isEqualTo(expectedPlan);

        CountPlan count = sqlExecutor.plan(stmt);
        assertThat(count.countPhase().where())
            .isFunction("op_>", isReference("x"), isLiteral(1));
    }

    @Test
    public void test_count_start_aggregate_filter_is_merged_with_where_clause_query() {
        var plan = plan(
            """
            SELECT COUNT(*) FILTER (WHERE x > 1)
            FROM t1
            WHERE x > 10
            """
        );
        var expectedPlan = "Count[doc.t1 | ((x > 10) AND (x > 1))]";
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_filter_on_pk_column_on_derived_table_is_optimized_to_get() {
        // the ORDER BY id, name is here to avoid a collect-then-fetch, which would (currently) break the Get optimization
        var plan = plan(
            """
            SELECT id, name
            FROM (
              SELECT id, name FROM users ORDER BY id, name) AS u
            WHERE id = 1 ORDER BY 1, 2
            """
        );
        var expectedPlan =
            """
            Rename[id, name] AS u
              └ OrderBy[id ASC name ASC]
                └ Get[doc.users | id, name | DocKeys{1::bigint} | (id = 1::bigint)]
            """;
        assertThat(plan).isEqualTo(expectedPlan);
    }

    @Test
    public void test_filter_on_relation_is_pushed_beneath_correlated_join() {
        var plan = sqlExecutor.logicalPlan(
            """
            SELECT a.mountain
            FROM sys.summits a
            WHERE
                EXISTS
                (
                    SELECT 1
                    FROM sys.summits b
                    WHERE
                        b.height = a.height
                )
                AND
                a.country = 'DE'
            """);
        assertThat(plan).hasOperators(
            "Eval[mountain]",
            "  └ Filter[EXISTS (SELECT 1 FROM (b))]",
            "    └ CorrelatedJoin[mountain, height, (SELECT 1 FROM (b))]",
            "      └ Rename[mountain, height] AS a",
            "        └ Collect[sys.summits | [mountain, height] | (country = 'DE')]",
            "      └ SubPlan",
            "        └ Rename[1] AS b",
            "          └ Limit[1;0]",
            "            └ Collect[sys.summits | [1] | (height = height)]"
        );
    }
}
