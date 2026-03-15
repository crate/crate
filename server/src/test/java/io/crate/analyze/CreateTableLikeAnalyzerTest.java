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
import io.crate.metadata.GeneratedReference;
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

    // --- Existing tests ---

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

    // --- Bare LIKE tests (excludes optional properties) ---

    @Test
    public void testBareLikeExcludesDefaults() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer, name text default 'unnamed')");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        Reference nameRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("name"))
            .findFirst().orElseThrow();
        assertThat(nameRef.defaultExpression()).isNull();
    }

    @Test
    public void testBareLikeExcludesGeneratedColumns() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int, b int as (a + 1))");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        Reference bRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("b"))
            .findFirst().orElseThrow();
        assertThat(bRef).isNotInstanceOf(GeneratedReference.class);
    }

    @Test
    public void testBareLikeExcludesCheckConstraints() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer check (id > 0))");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        assertThat(bound.checks()).isEmpty();
    }

    @Test
    public void testBareLikeExcludesPrimaryKeys() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer primary key, name text)");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        assertThat(bound.primaryKeys()).isEmpty();
    }

    @Test
    public void testBareLikePreservesNotNull() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer not null, name text)");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        Reference idRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("id"))
            .findFirst().orElseThrow();
        assertThat(idRef.isNullable()).isFalse();
    }

    @Test
    public void testBareLikeExcludesStorageSettings() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer) with (number_of_replicas = 2)");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        // bare LIKE should not copy storage settings; default replicas apply
        String replicas = bound.settings().get("index.number_of_replicas");
        assertThat(replicas).isNotEqualTo("2");
    }

    // --- INCLUDING option tests ---

    @Test
    public void testIncludingDefaultsCopiesDefaults() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer, name text default 'unnamed')");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl INCLUDING DEFAULTS)");
        Reference nameRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("name"))
            .findFirst().orElseThrow();
        assertThat(nameRef.defaultExpression()).isNotNull();
    }

    @Test
    public void testIncludingGeneratedCopiesGeneratedColumns() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (a int, b int as (a + 1))");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl INCLUDING GENERATED)");
        Reference bRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("b"))
            .findFirst().orElseThrow();
        assertThat(bRef).isInstanceOf(GeneratedReference.class);
    }

    @Test
    public void testIncludingConstraintsCopiesCheckConstraints() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer check (id > 0))");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl INCLUDING CONSTRAINTS)");
        assertThat(bound.checks()).isNotEmpty();
    }

    @Test
    public void testIncludingConstraintsCopiesPrimaryKey() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer primary key, name text)");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl INCLUDING CONSTRAINTS)");
        assertThat(bound.primaryKeys()).isNotEmpty();
        assertThat(bound.primaryKeys().get(0).column().name()).isEqualTo("id");
    }

    @Test
    public void testIncludingStorageCopiesTableSettings() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer) with (number_of_replicas = 2)");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl INCLUDING STORAGE)");
        assertThat(bound.settings().get("index.number_of_replicas")).isEqualTo("2");
    }

    // --- Combined option tests ---

    @Test
    public void testIncludingAllExcludingDefaultsDropsOnlyDefaults() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer check (id > 0), name text default 'unnamed', score int as (id + 1))");

        BoundCreateTable bound = bindCreateTableLike(e,
            "create table cpy (LIKE tbl INCLUDING ALL EXCLUDING DEFAULTS)");

        // Defaults excluded
        Reference nameRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("name"))
            .findFirst().orElseThrow();
        assertThat(nameRef.defaultExpression()).isNull();

        // Check constraints included
        assertThat(bound.checks()).isNotEmpty();

        // Generated columns included
        Reference scoreRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("score"))
            .findFirst().orElseThrow();
        assertThat(scoreRef).isInstanceOf(GeneratedReference.class);
    }

    @Test
    public void testIncludingAllExcludingConstraintsDropsChecksAndPKs() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer primary key check (id > 0), name text default 'unnamed')");

        BoundCreateTable bound = bindCreateTableLike(e,
            "create table cpy (LIKE tbl INCLUDING ALL EXCLUDING CONSTRAINTS)");

        // Constraints excluded
        assertThat(bound.checks()).isEmpty();
        assertThat(bound.primaryKeys()).isEmpty();

        // Defaults still included
        Reference nameRef = bound.columns().values().stream()
            .filter(r -> r.column().name().equals("name"))
            .findFirst().orElseThrow();
        assertThat(nameRef.defaultExpression()).isNotNull();
    }

    @Test
    public void testCreateTableLikePreservesPartitioning() throws IOException {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (id integer, p integer) partitioned by (p)");

        BoundCreateTable bound = bindCreateTableLike(e, "create table cpy (LIKE tbl)");
        assertThat(bound.partitionedBy()).isNotEmpty();
        assertThat(bound.partitionedBy().get(0).column().name()).isEqualTo("p");
    }
}
