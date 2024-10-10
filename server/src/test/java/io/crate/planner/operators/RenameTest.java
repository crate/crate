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

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.FieldResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RenameTest extends CrateDummyClusterServiceUnitTest {

    /**
     * https://github.com/crate/crate/issues/16754
     */
    @Test
    public void test_parent_child_column_prunning() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService).addTable("create table tbl (a int, b int)");

        Symbol a = e.asSymbol("a");
        Symbol b = e.asSymbol("b");
        Symbol childSymbol = e.asSymbol("b > 1");
        Symbol parentSymbol = e.asSymbol("b > 1");

        Collect source = new Collect(mock(AbstractTableRelation.class), List.of(a, b, childSymbol), WhereClause.MATCH_ALL);
        Rename rename = new Rename(List.of(a, b, parentSymbol), mock(RelationName.class), mock(FieldResolver.class), source);
        assertThat(rename.outputs()).containsExactly(a, b, parentSymbol);

        var pruned = rename.pruneOutputsExcept(List.of(a, b, parentSymbol));
        assertThat(pruned.outputs()).containsExactly(a, b, parentSymbol);

        pruned = rename.pruneOutputsExcept(List.of(a, b, childSymbol));
        assertThat(pruned.outputs()).containsExactly(a, b, parentSymbol);

    }
}
