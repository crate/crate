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

package io.crate.execution.dml.upsert;


import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import io.crate.analyze.Id;
import io.crate.execution.dml.IndexItem;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class UpdateToInsertTest extends CrateDummyClusterServiceUnitTest {

    private static Doc doc(String id, String index, Map<String, Object> source) {
        Supplier<String> rawSource = () -> {
            try {
                return Strings.toString(JsonXContent.builder().map(source));
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        };
        return new Doc(1, index, id, 1, 1, 1, source, rawSource);
    }

    @Test
    public void test_update_one_column_generates_all_insert_values() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "y" },
            null
        );
        Map<String, Object> source = Map.of("x", 10, "y", 5);
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);

        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(20) },
            new Object[0]
        );
        assertThat(item.insertValues())
            .containsExactly(10, 20);
    }

    @Test
    public void test_update_can_use_excluded_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "y" },
            null
        );
        Map<String, Object> source = Map.of("x", 10, "y", 5);
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);

        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { new InputColumn(0) },
            new Object[] { 20 }
        );
        assertThat(item.insertValues())
            .containsExactly(10, 20);
    }

    @Test
    public void test_can_assign_value_to_object_child() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, o object as (y int))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "o.y" },
            null
        );
        Map<String, Object> source = Map.of("x", 1, "o", Map.of("y", 2));
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(3) },
            new Object[] {}
        );
        assertThat(item.insertValues())
            .containsExactly(1, Map.of("y", 3));
    }

    @Test
    public void test_generated_columns_are_excluded() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int as x + 4)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "x" },
            null
        );
        Map<String, Object> source = Map.of("x", 1, "y", 5);
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(8) },
            new Object[] {}
        );
        assertThat(item.insertValues())
            .containsExactly(8);
    }

    @Test
    public void test_checks_are_ignored() throws Exception {
        /**
         * Checks can be ignored because the index operation afterwards will run them.
         **/
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int check (x > 10))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "x" },
            null
        );
        Map<String, Object> source = Map.of("x", 12);
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);

        Symbol[] assignments = new Symbol[] { Literal.of(8) };
        IndexItem item = updateToInsert.convert(doc, assignments, new Object[0]);
        assertThat(item.insertValues())
            .containsExactly(8);
    }

    @Test
    public void test_can_add_new_top_level_columns_via_update() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int) with (column_policy = 'dynamic')")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "x", "y" },
            null
        );
        Map<String, Object> source = Map.of("x", 12);
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(1), Literal.of(2) },
            new Object[] {}
        );
        assertThat(updateToInsert.columns()).satisfiesExactly(
            c -> assertThat(c).isReference().hasName("x"),
            c -> assertThat(c).isReference().hasName("y")
        );
        assertThat(item.insertValues())
            .containsExactly(1, 2);
    }

    @Test
    public void test_adds_nested_primary_key_value_to_pkValues() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int primary key), y int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "y" },
            null
        );
        Map<String, Object> source = Map.of("y", 1, "o", Map.of("x", 3));
        Doc doc = doc("3", table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(1) },
            new Object[] {}
        );
        assertThat(item.pkValues()).containsExactly("3");
    }

    @Test
    public void test_cannot_assign_to_nested_column_if_parent_is_missing() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int) with (column_policy = 'dynamic')")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        assertThatThrownBy(() -> new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "o.x" },
            null
        )).isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot add new child `o['x']` if parent column is missing");
    }

    @Test
    public void test_preserves_insert_column_order() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int, z int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        // INSERT INTO tbl (z) VALUES (?) ON CONFLICT (...) DO UPDATE SET y = ?
        Reference[] insertColumns = new Reference[] { table.getReference(new ColumnIdent("z")) };
        String[] updateColumns = new String[] { "y" };
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            updateColumns,
            Arrays.asList(insertColumns)
        );
        assertThat(updateToInsert.columns())
            .as("Start References of columns() must match insertColumns")
            .satisfiesExactly(
                x -> assertThat(x).isReference().hasName("z"),
                x -> assertThat(x).isReference().hasName("x"),
                x -> assertThat(x).isReference().hasName("y")
            );

        Map<String, Object> source = Map.of("x", 1, "y", 2, "z", 3);
        Doc doc = doc(UUIDs.randomBase64UUID(), table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(20) },
            new Object[] { Literal.of(3) }
        );
        assertThat(item.insertValues()).containsExactly(3, 1, 20);
    }

    /**
     * Tests a regression where the wrong column list was used resulting in OutOfBounds exceptions or updating the
     * wrong column.
     * See https://github.com/crate/crate/issues/14906.
     */
    @Test
    public void test_preserves_insert_column_order_when_updating_sub_column() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable("create table tbl (x int, y object as (a int), z int)")
                .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        // INSERT INTO tbl (z) VALUES (?) ON CONFLICT (...) DO UPDATE SET y['a'] = ?
        Reference[] insertColumns = new Reference[] { table.getReference(new ColumnIdent("z")) };
        String[] updateColumns = new String[] { "y.a" };
        UpdateToInsert updateToInsert = new UpdateToInsert(
                e.nodeCtx,
                new CoordinatorTxnCtx(e.getSessionSettings()),
                table,
                updateColumns,
                Arrays.asList(insertColumns)
        );
        assertThat(updateToInsert.columns())
                .as("Start References of columns() must match insertColumns")
                .satisfiesExactly(
                        x -> assertThat(x).isReference().hasName("z"),
                        x -> assertThat(x).isReference().hasName("x"),
                        x -> assertThat(x).isReference().hasName("y")
                );

        Map<String, Object> source = Map.of("x", 1, "y", Map.of("a", 2), "z", 3);
        String id = UUIDs.randomBase64UUID();
        Doc doc = doc(id, table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
                doc,
                new Symbol[] { Literal.of(20) },
                new Object[] { Literal.of(3) }
        );
        assertThat(item.insertValues()).containsExactly(3, 1, Map.of("a", 20));

    }

    @Test
    public void test_generates_missing_generated_pk_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    x int,
                    y int generated always as (x + 1),
                    z int,
                    primary key (x, y)
                )
                """)
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        // insert into tbl (x, z) values (1, 20) on conflict (..) do update set z = excluded.z
        Reference[] insertColumns = new Reference[] {
            table.getReference(new ColumnIdent("x")),
            table.getReference(new ColumnIdent("z"))
        };
        String[] updateColumns = new String[] { "z" };
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            updateColumns,
            Arrays.asList(insertColumns)
        );
        Map<String, Object> source = Map.of("x", 1, "y", 2, "z", 3);
        String id = Id.encode(List.of("1", "2"), -1);
        Doc doc = doc(id, table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { new InputColumn(1) },
            new Object[] { 1, 20 }
        );
        assertThat(updateToInsert.columns()).satisfiesExactly(
            x -> assertThat(x).isReference().hasName("x"),
            x -> assertThat(x).isReference().hasName("z")
        );
        assertThat(item.pkValues()).containsExactly("1", "2");
        assertThat(item.insertValues()).containsExactly(1, 20);
    }

    @Test
    public void test_generates_nested_missing_generated_pk_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    x int,
                    o object as (
                        y int generated always as (x + 1)
                    ),
                    z int,
                    primary key (x, o['y'])
                )
                """)
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        // insert into tbl (x, z) values (1, 20) on conflict (..) do update set z = excluded.z
        Reference[] insertColumns = new Reference[] {
            table.getReference(new ColumnIdent("x")),
            table.getReference(new ColumnIdent("z"))
        };
        String[] updateColumns = new String[] { "z" };
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            updateColumns,
            Arrays.asList(insertColumns)
        );
        Map<String, Object> source = Map.of(
            "x", 1,
            "o", Map.of("y", 2),
            "z", 3
        );
        String id = Id.encode(List.of("1", "2"), -1);
        Doc doc = doc(id, table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { new InputColumn(1) },
            new Object[] { 1, 20 }
        );
        assertThat(item.pkValues()).containsExactly("1", "2");
        assertThat(item.insertValues()).containsExactly(1, 20, Map.of("y", 2));
    }

    @Test
    public void test_generates_missing_pk_columns_with_default() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    x int,
                    y int default 10,
                    z int,
                    primary key (x, y)
                )
                """)
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        // insert into tbl (x, z) values (1, 20) on conflict (..) do update set z = excluded.z
        Reference[] insertColumns = new Reference[] {
            table.getReference(new ColumnIdent("x")),
            table.getReference(new ColumnIdent("z"))
        };
        String[] updateColumns = new String[] { "z" };
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            updateColumns,
            Arrays.asList(insertColumns)
        );
        Map<String, Object> source = Map.of(
            "x", 1,
            "y", 10,
            "z", 3
        );
        String id = Id.encode(List.of("1", "10"), -1);
        Doc doc = doc(id, table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { new InputColumn(1) },
            new Object[] { 1, 20 }
        );
        assertThat(item.pkValues()).containsExactly("1", "10");
        assertThat(item.insertValues()).containsExactly(1, 20, 10);
    }
}
