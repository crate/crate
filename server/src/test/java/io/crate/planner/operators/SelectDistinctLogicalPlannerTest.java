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
            "select distinct id from users order by id + 10::int");
        assertThat(logicalPlan, isPlan(
            "Eval[id]\n" +
            "  └ OrderBy[(id + 10) ASC]\n" +
            "    └ GroupHashAggregate[id]\n" +
            "      └ Collect[doc.users | [id] | true]"));
    }

    @Test
    public void testDistinctOnLiteralResultsInGroupByOnLiteral() {
        LogicalPlan plan = e.logicalPlan("select distinct [1, 2, 3] from users");
        assertThat(plan, isPlan(
            "GroupHashAggregate[[1, 2, 3]]\n" +
            "  └ Collect[doc.users | [[1, 2, 3]] | true]"));
    }

    @Test
    public void testOrderByOnColumnNotPresentInDistinctOutputsIsNotAllowed() {
        expectedException.expectMessage("Cannot ORDER BY `id`");
        e.plan("select distinct name from users order by id");
    }

    @Test
    public void testDistinctMixedWithTableFunctionInOutput() {
        LogicalPlan plan = e.logicalPlan("select distinct generate_series(1, 2), col1 from unnest([1, 1])");
        assertThat(plan, isPlan(
            "GroupHashAggregate[generate_series(1, 2), col1]\n" +
            "  └ ProjectSet[generate_series(1, 2), col1]\n" +
            "    └ TableFunction[unnest | [col1] | true]"));
    }

    @Test
    public void testDistinctOnJoinWithGroupByAddsAnotherGroupByOperator() {
        LogicalPlan logicalPlan = e.logicalPlan(
            "select distinct count(users.id) from users " +
            "inner join departments on users.department_id = departments.id " +
            "group by departments.name"
        );
        assertThat(logicalPlan, isPlan(
            "GroupHashAggregate[count(id)]\n" +
            "  └ GroupHashAggregate[name | count(id)]\n" +
            "    └ HashJoin[(department_id = id)]\n" +
            "      ├ Collect[doc.users | [id, department_id] | true]\n" +
            "      └ Collect[doc.departments | [name, id] | true]"
        ));
    }

    @Test
    public void testDistinctOnJoinWithoutGroupByAddsGroupByOperator() {
        LogicalPlan logicalPlan = e.logicalPlan(
            "select distinct departments.name from users " +
            "inner join departments on users.department_id = departments.id " +
            "order by departments.name");
        assertThat(logicalPlan, isPlan(
            "OrderBy[name ASC]\n" +
            "  └ GroupHashAggregate[name]\n" +
            "    └ HashJoin[(department_id = id)]\n" +
            "      ├ Collect[doc.users | [department_id] | true]\n" +
            "      └ Collect[doc.departments | [name, id] | true]"
        ));
    }
}
