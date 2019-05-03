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

import static io.crate.planner.operators.LogicalPlannerTest.isPlan;

public class SelectDistinctLogicalPlannerTest extends CrateDummyClusterServiceUnitTest  {

    private SQLExecutor e;

    @Before
    public void createExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table users (id int, department_id int, name string)")
            .addTable("create table departments (id int, name string)")
            .build();
    }

    @Test
    public void testOrderByCanContainScalarThatIsNotInDistinctOutputs() {
        LogicalPlan logicalPlan = e.logicalPlan(
            "select distinct id from users order by id + 10");
        assertThat(logicalPlan, isPlan(e.functions(),
            "RootBoundary[id]\n" +
            "FetchOrEval[id]\n" +
            "OrderBy[(id + 10) ASC]\n" +
            "GroupBy[id | ]\n" +
            "Collect[doc.users | [id, (id + 10)] | All]\n"));
    }

    @Test
    public void testDistinctOnLiteralResultsInGroupByOnLiteral() {
        LogicalPlan plan = e.logicalPlan("select distinct [1, 2, 3] from users");
        assertThat(plan, isPlan(e.functions(),
            "RootBoundary[[1, 2, 3]]\n" +
            "GroupBy[[1, 2, 3] | ]\n" +
            "Collect[doc.users | [[1, 2, 3]] | All]\n"));
    }

    @Test
    public void testOrderByOnColumnNotPresentInDistinctOutputsIsNotAllowed() {
        expectedException.expectMessage("Cannot order by \"id\"");
        e.plan("select distinct name from users order by id");
    }

    @Test
    public void testDistinctMixedWithTableFunctionInOutput() {
        LogicalPlan plan = e.logicalPlan("select distinct generate_series(1, 2), col1 from unnest([1, 1])");
        assertThat(plan, isPlan(e.functions(),
            "RootBoundary[generate_series(1, 2), col1]\n" +
            "GroupBy[generate_series(1, 2), col1 | ]\n" +
            "ProjectSet[generate_series(1, 2) | col1]\n" +
            "Collect[.unnest | [col1] | All]\n"));
    }

    @Test
    public void testDistinctOnJoinWithGroupByAddsAnotherGroupByOperator() {
        LogicalPlan logicalPlan = e.logicalPlan(
            "select distinct count(users.id) from users " +
            "inner join departments on users.department_id = departments.id " +
            "group by departments.name"
        );
        assertThat(logicalPlan, isPlan(e.functions(),
            "RootBoundary[count(id)]\n" +
            "GroupBy[count(id) | ]\n" +
            "GroupBy[name | count(id)]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, id, department_id]\n" +
            "    FetchOrEval[_fetchid, id, department_id]\n" +
            "    Collect[doc.users | [_fetchid, department_id, id] | All]\n" +
            "    --- INNER ---\n" +
            "    Boundary[id, name]\n" +
            "    Collect[doc.departments | [id, name] | All]\n" +
            "]\n"));
    }

    @Test
    public void testDistinctOnJoinWithoutGroupByAddsGroupByOperator() {
        LogicalPlan logicalPlan = e.logicalPlan(
            "select distinct departments.name from users " +
            "inner join departments on users.department_id = departments.id " +
            "order by departments.name");
        assertThat(logicalPlan, isPlan(e.functions(),
            "RootBoundary[name]\n" +
            "OrderBy[name ASC]\n" +
            "GroupBy[name | ]\n" +
            "HashJoin[\n" +
            "    Boundary[_fetchid, department_id]\n" +
            "    Collect[doc.users | [_fetchid, department_id] | All]\n" +
            "    --- INNER ---\n" +
            "    Boundary[id, name]\n" +
            "    Collect[doc.departments | [id, name] | All]\n" +
            "]\n"));
    }
}
