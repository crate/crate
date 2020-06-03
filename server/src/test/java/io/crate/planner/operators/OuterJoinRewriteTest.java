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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

public class OuterJoinRewriteTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void setup() throws IOException {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x int)")
            .build();
    }

    @Test
    public void testFilterAndOuterJoinIsRewrittenToInnerJoinIfFilterEliminatesNullRow() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.x " +
            "WHERE t2.x = '10'"
        );
        var expectedPlan =
            "NestedLoopJoin[INNER | (x = x)]\n" +
            "  ├ Collect[doc.t1 | [x] | true]\n" +
            "  └ Collect[doc.t2 | [x] | (x = 10)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterAndOuterJoinIsNotRewrittenToInnerJoinIfFilterDoesNot() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.x " +
            "WHERE coalesce(t2.x, 10) = 10"
        );
        var expectedPlan =
            "Filter[(coalesce(x, 10) = 10)]\n" +
            "  └ NestedLoopJoin[LEFT | (x = x)]\n" +
            "    ├ Collect[doc.t1 | [x] | true]\n" +
            "    └ Collect[doc.t2 | [x] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnLeftOuterJoinIsPartiallyPushedDownToTheLeftSide() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.x = t2.x " +
            "WHERE coalesce(t2.x, 10) = 10 AND t1.x > 5"
        );
        var expectedPlan =
            "Filter[(coalesce(x, 10) = 10)]\n" +
            "  └ NestedLoopJoin[LEFT | (x = x)]\n" +
            "    ├ Collect[doc.t1 | [x] | (x > 5)]\n" +
            "    └ Collect[doc.t2 | [x] | true]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnRightOuterJoinIsPartiallyPushedDownToTheRightSide() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM t1 RIGHT JOIN t2 ON t1.x = t2.x " +
            "WHERE coalesce(t1.x, 10) = 10 AND t2.x > 5"
        );
        var expectedPlan =
            "Filter[(coalesce(x, 10) = 10)]\n" +
            "  └ NestedLoopJoin[RIGHT | (x = x)]\n" +
            "    ├ Collect[doc.t1 | [x] | true]\n" +
            "    └ Collect[doc.t2 | [x] | (x > 5)]";
        assertThat(plan, isPlan(expectedPlan));
    }

    @Test
    public void testFilterOnFullOuterJoinIsPartiallyPushedDownToTheRightSide() {
        var plan = sqlExecutor.logicalPlan(
            "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.x = t2.x " +
            "WHERE coalesce(t1.x, 10) = 10 AND t2.x > 5"
        );
        var expectedPlan =
            "Filter[((coalesce(x, 10) = 10) AND (x > 5))]\n" +
            "  └ NestedLoopJoin[FULL | (x = x)]\n" +
            "    ├ Collect[doc.t1 | [x] | true]\n" +
            "    └ Collect[doc.t2 | [x] | (x > 5)]";
        assertThat(plan, isPlan(expectedPlan));
    }
}
