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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.junit.Test;

import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class IndexerTest extends CrateDummyClusterServiceUnitTest {

    static IndexItem item(Object ... values) {
        return new IndexItem.StaticItem("dummy-id-1", List.of(), values, 0L, 0L);
    }

    static Indexer getIndexer(SQLExecutor e, String tableName, FieldType fieldType, String ... columns) {
        DocTableInfo table = e.resolveTableInfo(tableName);
        return new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            e.nodeCtx,
            column -> fieldType,
            Stream.of(columns)
                .map(x -> table.getReference(new ColumnIdent(x)))
                .toList(),
            null
        );
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
                col1 -> assertThat(col1).isReference("o['xs']", new ArrayType<>(DataTypes.LONG))
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
            .isEqualTo("{}");
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
            {"x":20}
            """);
    }

    @Test
    public void test_does_not_allow_new_columns_in_strict_object() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    o object (strict) as (
                        x int
                    )
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
                table.getReference(new ColumnIdent("o"))
            ),
            null
        );
        assertThatThrownBy(() -> indexer.index(item(Map.of("x", 10, "y", 20))))
            .hasMessage("Cannot add column `y` to strict object `o`");
    }

    @Test
    public void test_dynamic_int_value_results_in_long_column() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    o object (dynamic) as (
                        x int
                    )
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
                table.getReference(new ColumnIdent("o"))
            ),
            null
        );
        ParsedDocument index = indexer.index(item(Map.of("x", 10, "y", 20)));
        assertThat(index.newColumns()).satisfiesExactly(
            r -> assertThat(r).isReference("o['y']", DataTypes.LONG)
        );
    }

    @Test
    public void test_can_generate_return_values() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int default 20)")
            .build();

        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            e.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("x"))
            ),
            new Symbol[] {
                table.getReference(new ColumnIdent("_id")),
                table.getReference(new ColumnIdent("x")),
                table.getReference(new ColumnIdent("y")),
            }
        );

        Object[] returnValues = indexer.returnValues(item(10));
        assertThat(returnValues).containsExactly("dummy-id-1", 10, 20);
    }

    @Test
    public void test_fields_are_ommitted_in_source_for_null_values() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, o object as (y int))")
            .build();

        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            e.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("x")),
                table.getReference(new ColumnIdent("o"))
            ),
            null
        );

        HashMap<String, Object> o = new HashMap<>();
        o.put("y", null);
        ParsedDocument doc = indexer.index(item(null, o));
        assertThat(doc.source().utf8ToString()).isEqualTo(
            "{\"o\":{}}"
        );
    }

    @Test
    public void test_indexing_float_results_in_float_point() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x float)")
            .build();

        Indexer indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "x");
        ParsedDocument doc = indexer.index(item(42.2f));
        IndexableField[] fields = doc.doc().getFields("x");
        assertThat(fields).satisfiesExactly(
            x -> assertThat(x).isExactlyInstanceOf(FloatPoint.class),
            x -> assertThat(x)
                .isExactlyInstanceOf(SortedNumericDocValuesField.class)
                .extracting("fieldsData")
                .isEqualTo((long) NumericUtils.floatToSortableInt(42.2f))
        );
    }

    @Test
    public void test_can_index_fulltext_column() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x text index using fulltext with (analyzer = 'english'))")
            .build();

        var indexer = getIndexer(e, "tbl", TextFieldMapper.Defaults.FIELD_TYPE, "x");
        ParsedDocument doc = indexer.index(item("Hello World"));
        IndexableField[] fields = doc.doc().getFields("x");
        assertThat(fields).satisfiesExactly(
            x -> assertThat(x)
                .isExactlyInstanceOf(Field.class)
                .extracting("fieldsData")
                .as("value is indexed as string instead of BytesRef")
                .isEqualTo("Hello World")
        );
        assertThat(fields[0].fieldType().tokenized()).isTrue();
    }

    @Test
    public void test_can_index_all_storable_types() throws Exception {
        StringBuilder stmtBuilder = new StringBuilder()
            .append("create table tbl (");

        ArrayList<ColumnIdent> columns = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        List<DataType<?>> types = Lists2
            .concat(
                DataTypes.PRIMITIVE_TYPES,
                List.of(
                    DataTypes.GEO_POINT,
                    DataTypes.GEO_SHAPE,
                    new BitStringType(1)
                ))
            .stream()
            .filter(t -> t.storageSupport() != null)
            .toList();
        Iterator<DataType<?>> it = types.iterator();
        boolean first = true;
        while (it.hasNext()) {
            var type = it.next();
            if (first) {
                first = false;
            } else {
                stmtBuilder.append(",\n");
            }
            Object value = DataTypeTesting.getDataGenerator(type).get();
            values.add(value);
            columns.add(new ColumnIdent("c_" + type.getName()));
            stmtBuilder
                .append("\"c_" + type.getName() + "\" ")
                .append(type);

        }

        String stmt = stmtBuilder.append(")").toString();
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable(stmt)
            .build();

        DocTableInfo table = e.resolveTableInfo("tbl");
        try (var indexEnv = new IndexEnv(
                THREAD_POOL,
                table,
                clusterService.state(),
                Version.CURRENT,
                createTempDir())) {

            Indexer indexer = new Indexer(
                table.ident().indexNameOrAlias(),
                table,
                new CoordinatorTxnCtx(e.getSessionSettings()),
                e.nodeCtx,
                column -> indexEnv.mapperService().getLuceneFieldType(column.fqn()),
                Lists2.map(columns, c -> table.getReference(c)),
                null
            );
            ParsedDocument doc = indexer.index(item(values.toArray()));
            Map<String, Object> source = XContentHelper.toMap(doc.source(), XContentType.JSON);
            it = types.iterator();
            for (int i = 0; it.hasNext(); i++) {
                var type = it.next();
                Object expected = values.get(i);
                assertThat(source).hasEntrySatisfying(
                    "c_" + type.getName(),
                    v -> assertThat(type.sanitizeValue(v)).isEqualTo(expected)
                );
            }
        }
    }

    @Test
    public void test_can_add_dynamic_ref_as_new_top_level_column() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int) with (column_policy = 'dynamic')")
            .build();

        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            e.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("x")),
                table.getDynamic(new ColumnIdent("y"), true, false),
                table.getDynamic(new ColumnIdent("z"), true, false)
            ),
            null
        );
        ParsedDocument doc = indexer.index(item(42, "Hello", 21));
        assertThat(doc.newColumns()).satisfiesExactly(
            x -> assertThat(x).isReference("y", DataTypes.STRING),
            x -> assertThat(x).isReference("z", DataTypes.LONG)
        );
        assertThat(doc.source().utf8ToString()).isEqualToIgnoringWhitespace(
            """
            {"x": 42, "y": "Hello", "z": 21}
            """
        );

        doc = indexer.index(item(42, "Hello", 22));
        assertThat(doc.newColumns())
            .as("Doesn't repeatedly add new column")
            .hasSize(0);
    }

    @Test
    public void test_source_includes_null_values_in_arrays() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (xs int[])")
            .build();

        var indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "xs");
        ParsedDocument doc = indexer.index(item(Arrays.asList(1, 42, null, 21)));
        assertThat(doc.source().utf8ToString()).isEqualToIgnoringWhitespace(
            """
            {"xs": [1, 42, null, 21]}
            """
        );
    }
}
