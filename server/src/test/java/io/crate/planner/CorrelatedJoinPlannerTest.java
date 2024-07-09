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

import java.util.List;

import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.OuterColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.CorrelatedJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

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
            "Eval[('Mountain-' || (SELECT mountain FROM (empty_row)))]\n" +
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


    // Tracks https://github.com/crate/crate/issues/15901
    @Test
    public void test_prune_removes_unused_outputs_from_correlated_join() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int, b int)");
        DocTableInfo tbl = e.resolveTableInfo("tbl");
        var tableRelation = new DocTableRelation(tbl);

        Symbol a = e.asSymbol("a");
        Symbol b = e.asSymbol("b");

        var relation = new QueriedSelectRelation(
            false,
            List.of(tableRelation),
            List.of(),
            List.of(new OuterColumn(tableRelation, b)),
            Literal.BOOLEAN_TRUE,
            List.of(),
            null,
            null,
            null,
            null
        );

        Collect collect = new Collect(tableRelation, List.of(a, b), WhereClause.MATCH_ALL);
        SelectSymbol selectSymbol = new SelectSymbol(
            relation,
            new ArrayType<>(DataTypes.INTEGER),
            SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE,
            true
        );
        CorrelatedJoin join = new CorrelatedJoin(collect, selectSymbol, null);
        assertThat(join.outputs()).containsExactly(a, b, selectSymbol);
        LogicalPlan pruned = join.pruneOutputsExcept(List.of(b, selectSymbol));
        assertThat(pruned.outputs())
            .as("removes unused a")
            .containsExactly(b, selectSymbol);
    }
}
