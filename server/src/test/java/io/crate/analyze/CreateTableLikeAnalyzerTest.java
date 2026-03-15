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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;

public class CreateTableLikeAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private BoundCreateTable bindCreateTable(SQLExecutor e, String sql) {
        return ((AnalyzedCreateTable) e.analyze(sql)).bind(
            new NumberOfShards(clusterService),
            e.fulltextAnalyzerResolver(),
            e.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
    }

    private BoundCreateTable bindCreateTableLike(SQLExecutor e, String sql) {
        AnalyzedCreateTableLike analyzed = e.analyze(sql);
        return analyzed.analyzedCreateTable().bind(
            new NumberOfShards(clusterService),
            e.fulltextAnalyzerResolver(),
            e.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY
        );
    }

    @Test
    public void testSimpleCompareAgainstAnalyzedCreateTable() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(
                "create table tbl (" +
                "   col_default_object object as (" +
                "       col_nested_integer integer," +
                "       col_nested_object object as (" +
                "           col_nested_timestamp_with_time_zone timestamp with time zone" +
                "       )" +
                "   )" +
                ")"
            );

        BoundCreateTable expected = bindCreateTable(e,
            "create table cpy (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")"
        );

        BoundCreateTable actual = bindCreateTableLike(e, "create table cpy (LIKE tbl)");

        assertThat(actual.ifNotExists()).isFalse();
        assertThat(actual.tableName()).isEqualTo(expected.tableName());
        assertThat(actual.columns().keySet()).containsExactlyElementsOf(expected.columns().keySet());
        List<DataType<?>> expectedTypes = Lists.map(expected.columns().values(), Reference::valueType);
        List<DataType<?>> actualTypes = Lists.map(actual.columns().values(), Reference::valueType);
        assertThat(actualTypes).containsExactlyElementsOf(expectedTypes);
    }

    @Test
    public void testAnalyzedCreateTableLikeWithNotExists() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int)");

        BoundCreateTable actual = bindCreateTableLike(e, "create table if not exists cpy (LIKE tbl)");
        assertThat(actual.ifNotExists()).isTrue();
    }

    @Test
    public void testCreateTableLikeRejectsViews() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int)")
            .addView(
                io.crate.metadata.RelationName.fromIndexName("doc.v"),
                "select a from doc.tbl"
            );

        assertThatThrownBy(() -> e.analyze("create table cpy (LIKE v)"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessageContaining("doesn't support or allow SHOW CREATE operations");
    }

    @Test
    public void testCreateTableLikeRejectsSystemTables() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService);

        assertThatThrownBy(() -> e.analyze("create table cpy (LIKE sys.cluster)"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessageContaining("doesn't support or allow SHOW CREATE operations");
    }

    @Test
    public void testCreateTableLikePreservesSourceTableInfo() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int)");

        AnalyzedCreateTableLike analyzed = e.analyze("create table cpy (LIKE tbl)");
        assertThat(analyzed.sourceTableInfo()).isNotNull();
        assertThat(analyzed.sourceTableInfo().ident().name()).isEqualTo("tbl");
    }
}
