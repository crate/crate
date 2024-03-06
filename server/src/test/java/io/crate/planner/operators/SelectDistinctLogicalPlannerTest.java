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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SelectDistinctLogicalPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void createExecutor() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE users (id int, department_id int, name string)")
            .addTable("CREATE TABLE departments (id int, name string)");
    }

    @Test
    public void testOrderByCanContainScalarThatIsNotInDistinctOutputs() {
        LogicalPlan logicalPlan = e.logicalPlan(
            "SELECT DISTINCT id FROM users ORDER BY id + 10::int");
        assertThat(logicalPlan).isEqualTo(
            """
            Eval[id]
              └ OrderBy[(id + 10) ASC]
                └ GroupHashAggregate[id]
                  └ Collect[doc.users | [id] | true]
            """
        );
    }

    @Test
    public void testDistinctOnLiteralResultsInGroupByOnLiteral() {
        LogicalPlan plan = e.logicalPlan("SELECT DISTINCT [1, 2, 3] FROM users");
        assertThat(plan).isEqualTo(
            """
            GroupHashAggregate[[1, 2, 3]]
              └ Collect[doc.users | [[1, 2, 3]] | true]
            """
        );
    }

    @Test
    public void testOrderByOnColumnNotPresentInDistinctOutputsIsNotAllowed() {
        assertThatThrownBy(() -> e.plan("SELECT DISTINCT name FROM users ORDER BY id"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot ORDER BY `id`, the column does not appear in the outputs of the underlying relation");
    }

    @Test
    public void testDistinctMixedWithTableFunctionInOutput() {
        LogicalPlan plan = e.logicalPlan("SELECT DISTINCT generate_series(1, 2), unnest FROM unnest([1, 1])");
        assertThat(plan).isEqualTo(
            """
            GroupHashAggregate[pg_catalog.generate_series(1, 2), unnest]
              └ ProjectSet[pg_catalog.generate_series(1, 2), unnest]
                └ TableFunction[unnest | [unnest] | true]
            """
        );
    }

    @Test
    public void testDistinctOnJoinWithGroupByAddsAnotherGroupByOperator() {
        LogicalPlan logicalPlan = e.logicalPlan(
            """
            SELECT DISTINCT count(users.id) FROM users
            INNER JOIN departments ON users.department_id = departments.id
            GROUP BY departments.name
            """
        );
        assertThat(logicalPlan).isEqualTo(
            """
            GroupHashAggregate[count(id)]
              └ GroupHashAggregate[name | count(id)]
                └ HashJoin[(department_id = id)]
                  ├ Collect[doc.users | [id, department_id] | true]
                  └ Collect[doc.departments | [name, id] | true]
            """
        );
    }

    @Test
    public void testDistinctOnJoinWithoutGroupByAddsGroupByOperator() {
        LogicalPlan logicalPlan = e.logicalPlan(
            """
            SELECT DISTINCT departments.name FROM users
            INNER JOIN departments ON users.department_id = departments.id
            ORDER BY departments.name
            """
        );
        assertThat(logicalPlan).isEqualTo(
            """
            OrderBy[name ASC]
              └ GroupHashAggregate[name]
                └ HashJoin[(department_id = id)]
                  ├ Collect[doc.users | [department_id] | true]
                  └ Collect[doc.departments | [name, id] | true]
            """
        );
    }
}
