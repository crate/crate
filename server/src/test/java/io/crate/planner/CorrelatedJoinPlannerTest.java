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

package io.crate.planner;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.planner.operators.LogicalPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CorrelatedJoinPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_correlated_subquery_without_using_alias_can_use_outer_column_in_where_clause() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        String statement = "SELECT (SELECT mountain) FROM sys.summits ORDER BY 1 ASC LIMIT 5";
        LogicalPlan result = e.logicalPlan(statement);
        assertThat(result).isEqualTo(
            "Eval[(SELECT mountain FROM (empty_row))]\n" +
            "  └ Limit[5::bigint;0]\n" +
            "    └ OrderBy[(SELECT mountain FROM (empty_row)) ASC]\n" +
            "      └ CorrelatedJoin[mountain, (SELECT mountain FROM (empty_row))]\n" +
            "        └ Collect[sys.summits | [mountain] | true]\n" +
            "        └ SubPlan\n" +
            "          └ Eval[mountain]\n" +
            "            └ Limit[2::bigint;0::bigint]\n" +
            "              └ TableFunction[empty_row | [] | true]"
        );
    }

    @Test
    public void test_correlated_subquery_within_scalar() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        String statement = "SELECT 'Mountain-' || (SELECT t.mountain) FROM sys.summits t";
        LogicalPlan logicalPlan = e.logicalPlan(statement);
        assertThat(logicalPlan).isEqualTo(
            "Eval[concat('Mountain-', (SELECT mountain FROM (empty_row)))]\n" +
            "  └ CorrelatedJoin[mountain, (SELECT mountain FROM (empty_row))]\n" +
            "    └ Rename[mountain] AS t\n" +
            "      └ Collect[sys.summits | [mountain] | true]\n" +
            "    └ SubPlan\n" +
            "      └ Eval[mountain]\n" +
            "        └ Limit[2::bigint;0::bigint]\n" +
            "          └ TableFunction[empty_row | [] | true]"
        );
    }

    @Test
    public void test_correlated_subquery_cannot_be_used_in_group_by() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        String statement = "SELECT (SELECT t.mountain), count(*) FROM sys.summits t GROUP BY (SELECT t.mountain)";
        assertThatThrownBy(() -> e.plan(statement))
            .hasMessage("Cannot use correlated subquery in GROUP BY clause");
    }

    @Test
    public void test_correlated_subquery_cannot_be_used_in_having() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        String stmt = "SELECT (SELECT t.mountain), count(*) FROM sys.summits t HAVING (SELECT t.mountain) = 'Acherkogel'";
        assertThatThrownBy(() -> e.plan(stmt))
            .hasMessage("Cannot use correlated subquery in HAVING clause");
    }
}
