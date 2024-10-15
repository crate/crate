/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TableFunctionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_column_pruning_with_empty_outputs() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService).addTable("create table tbl (a int, b int)");

        Symbol a = e.asSymbol("a");
        Symbol b = e.asSymbol("b");

        LogicalPlan tableFunction = TableFunction.create(mock(TableFunctionRelation.class), List.of(), WhereClause.MATCH_ALL);
        assertThat(tableFunction.outputs()).isEmpty();
        var prunedTableFunction = tableFunction.pruneOutputsExcept(List.of(a, b));
        assertThat(prunedTableFunction.outputs()).containsExactly(a, b);
    }

    @Test
    public void test_column_pruning_with_more_outputs() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService).addTable("create table tbl (a int, b int, c int)");

        Symbol a = e.asSymbol("a");
        Symbol b = e.asSymbol("b");
        Symbol c = e.asSymbol("c");

        LogicalPlan tableFunction = TableFunction.create(mock(TableFunctionRelation.class), List.of(a, b, c), WhereClause.MATCH_ALL);
        assertThat(tableFunction.outputs()).containsExactly(a, b, c);
        var prunedTableFunction = tableFunction.pruneOutputsExcept(List.of(a, b));
        assertThat(prunedTableFunction.outputs()).containsExactly(a, b);
    }

}
