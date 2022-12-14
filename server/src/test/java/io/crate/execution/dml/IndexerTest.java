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

package io.crate.execution.dml;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class IndexerTest extends CrateDummyClusterServiceUnitTest {

    static IndexItem item(Object ... values) {
        return new IndexItem.StaticItem("dummy-id-1", List.of(),values, 0L, 0L);
    }

    @Test
    public void test_index_object_with_dynamic_column_creation() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o),
            null
        );

        Map<String, Object> value = Map.of("x", 10, "y", 20);
        ParsedDocument parsedDoc = indexer.index(item(value));
        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);

        assertThat(parsedDoc.newColumns())
            .hasSize(1);

        assertThat(parsedDoc.source().utf8ToString()).isIn(
            "{\"o\":{\"x\":10,\"y\":20}}",
            "{\"o\":{\"y\":20,\"x\":10}}"
        );
    }

    @Test
    public void test_create_dynamic_object_with_nested_columns() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o),
            null
        );

        Map<String, Object> value = Map.of("x", 10, "obj", Map.of("y", 20));
        ParsedDocument parsedDoc = indexer.index(item(value));
        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);

        assertThat(parsedDoc.newColumns())
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference("o['obj']"),
                col2 -> assertThat(col2).isReference("o['obj']['y']")
            );
    }

    @Test
    public void test_create_dynamic_array() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o),
            null
        );

        Map<String, Object> value = Map.of("x", 10, "xs", List.of(2, 3, 4));
        ParsedDocument parsedDoc = indexer.index(item(value));

        assertThat(parsedDoc.newColumns())
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference("o['xs']", new ArrayType<>(DataTypes.INTEGER))
            );

        assertThat(parsedDoc.source().utf8ToString()).isIn(
            "{\"o\":{\"x\":10,\"xs\":[2,3,4]}}",
            "{\"o\":{\"xs\":[2,3,4],\"x\":10}}"
        );

        assertThat(parsedDoc.doc().getFields())
            .hasSize(14);
    }

    @Test
    public void test_adds_default_values() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int default 0)")
            .build();
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(executor.getSessionSettings());
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Reference y = table.getReference(new ColumnIdent("y"));
        var indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            txnCtx,
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(y),
            null
        );
        var parsedDoc = indexer.index(item(new Object[] { null }));
        assertThat(parsedDoc.source().utf8ToString())
            .as("If explicit null value is provided, the default expression is not applied")
            .isEqualTo( "{\"y\":null}");
        assertThat(parsedDoc.doc().getFields())
            .hasSize(6);

        indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            txnCtx,
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x),
            null
        );
        parsedDoc = indexer.index(item(10));
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"x\":10,\"y\":0}"
        );
        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);
    }

    @Test
    public void test_adds_generated_column() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int as x + 2)")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x),
            null
        );
        var parsedDoc = indexer.index(item(1));
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"x\":1,\"y\":3}"
        );
    }

    @Test
    public void test_generated_partitioned_column_is_not_indexed_or_included_in_source() throws Exception {
        String partition = new PartitionName(new RelationName("doc", "tbl"), List.of("3")).asIndexName();
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addPartitionedTable(
                "create table doc.tbl (x int, p int as x + 2) partitioned by (p)",
                partition
            )
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Indexer indexer = new Indexer(
            partition,
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x),
            null
        );
        var parsedDoc = indexer.index(item(1));
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"x\":1}"
        );
    }

    @Test
    public void test_default_and_generated_column_within_object() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int default 0, y int as o['x'] + 2, z int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o),
            null
        );

        var parsedDoc = indexer.index(item(Map.of("z", 20)));
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"o\":{\"x\":0,\"y\":2,\"z\":20}}"
        );
    }

    @Test
    public void test_default_for_full_object() throws Exception {
        var executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, o object as (x int) default {x=10})")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x),
            null
        );
        var parsedDoc = indexer.index(item(42));
        assertThat(parsedDoc.source().utf8ToString()).isEqualToIgnoringWhitespace("""
            {
                "x":42,
                "o": {
                    "x": 10
                }
            }
        """);
    }

    @Test
    public void test_validates_user_provided_value_for_generated_columns() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int as x + 2, o object as (z int as x + 3))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Reference y = table.getReference(new ColumnIdent("y"));
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer1 = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x, y),
            null
        );
        assertThatThrownBy(() -> indexer1.index(item(1, 2)))
            .hasMessage("Given value 2 for generated column y does not match calculation (x + 2) = 3");

        Indexer indexer2 = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x, o),
            null
        );
        assertThatThrownBy(() -> indexer2.index(item(1, Map.of("z", 10))))
            .hasMessage("Given value 10 for generated column o['z'] does not match calculation (x + 3) = 4");
    }

    @Test
    public void test_index_fails_if_not_null_column_has_null_value() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int not null, y int default 0 NOT NULL)")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("x"))
            ),
            null
        );
        assertThatThrownBy(() -> indexer.index(item(new Object[] { null })))
            .hasMessage("\"x\" must not be null");

        ParsedDocument parsedDoc = indexer.index(item(10));
        assertThat(parsedDoc.source().utf8ToString()).isEqualToIgnoringWhitespace("""
            {"x":10, "y":0}
        """);
    }

    @Test
    public void test_index_fails_if_check_constraint_returns_false() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    x int not null constraint c1 check (x > 10),
                    y int constraint c2 check (y < 3),
                    z int default 0 check (z > 0)
                )
                """)
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("x")),
                table.getReference(new ColumnIdent("z"))
            ),
            null
        );
        assertThatThrownBy(() -> indexer.index(item(8, 10)))
            .hasMessage("Failed CONSTRAINT c1 CHECK (\"x\" > 10)");

        ParsedDocument parsedDoc = indexer.index(item(20, null));
        assertThat(parsedDoc.source().utf8ToString()).isEqualToIgnoringWhitespace("""
            {"x":20, "z":null}
        """);
    }
}
