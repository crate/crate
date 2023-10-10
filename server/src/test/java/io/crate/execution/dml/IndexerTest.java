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

import static io.crate.metadata.doc.mappers.array.ArrayMapperTest.mapper;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.junit.Ignore;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.common.collections.Lists2;
import io.crate.common.collections.MapBuilder;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.execution.ddl.tables.AddColumnTask;
import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.sql.tree.BitString;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.IndexEnv;
import io.crate.testing.SQLExecutor;
import io.crate.testing.UseNewCluster;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.IpType;
import io.crate.types.ObjectType;

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
                .map(x -> table.resolveColumn(x, true, false))
                .toList(),
            null
        );
    }

    private DocTableInfo addColumns(SQLExecutor e, DocTableInfo table, List<Reference> newColumns) throws Exception {
        try (IndexEnv indexEnv = new IndexEnv(
                THREAD_POOL,
                table,
                clusterService.state(),
                Version.CURRENT,
                createTempDir()
        )) {
            var addColumnTask = new AddColumnTask(e.nodeCtx, imd -> indexEnv.mapperService());
            AddColumnRequest request = new AddColumnRequest(
                    table.ident(),
                    newColumns,
                    Map.of(),
                    new IntArrayList(0)
            );
            ClusterState newState = addColumnTask.execute(clusterService.state(), request);
            return new DocTableInfoFactory(e.nodeCtx).create(table.ident(), newState);
        }
    }

    static Map<String, Object> sourceMap(ParsedDocument parsedDocument, DocTableInfo tableInfo) throws Exception {
        var sourceParser = new SourceParser(tableInfo.droppedColumns(), tableInfo.lookupNameBySourceKey());
        return sourceParser.parse(parsedDocument.source());
    }

    static String source(ParsedDocument parsedDocument, DocTableInfo tableInfo) throws Exception {
        return Strings.toString(
                XContentBuilder.builder(JsonXContent.JSON_XCONTENT)
                        .map(sourceMap(parsedDocument, tableInfo))
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
        IndexItem item = item(value);
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);
        DocTableInfo actualTable = addColumns(executor, table, newColumns);
        indexer.updateTargets(actualTable::getReference);
        ParsedDocument parsedDoc = indexer.index(item);
        assertThat(parsedDoc.doc().getFields())
            .hasSize(8);

        assertThat(newColumns)
            .hasSize(1);

        assertThat(source(parsedDoc, actualTable)).isIn(
            "{\"o\":{\"x\":10,\"y\":20}}",
            "{\"o\":{\"y\":20,\"x\":10}}"
        );

        value = Map.of("x", 10, "y", 20);
        newColumns = indexer.collectSchemaUpdates(item(value));
        assertThat(newColumns).isEmpty();
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

        Map<String, Object> value = Map.of("x", 10, "obj", Map.of("y", 20, "z", 30));
        IndexItem item = item(value);
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);

        // Add new columns so they get
        //  1. an OID applied
        //  2. the correct data type, in this case the `o['obj']` must contain its new members 'y' + 'z'
        DocTableInfo actualTable = addColumns(executor, table, newColumns);

        indexer.updateTargets(actualTable::getReference);
        ParsedDocument parsedDoc = indexer.index(item);
        assertThat(parsedDoc.doc().getFields())
            .hasSize(9);

        assertThat(newColumns)
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference().hasName("o['obj']"),
                col2 -> assertThat(col2).isReference()
                    .hasName("o['obj']['y']"),
                col3 -> assertThat(col3).isReference()
                    .hasName("o['obj']['z']")
            );
    }

    @Test
    public void test_ignored_object_values_are_ignored_and_added_to_source() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object (ignored))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        var indexer = getIndexer(e, "tbl", null, "o");
        ParsedDocument doc = indexer.index(item(Map.of("x", 10)));
        assertThat(source(doc, table)).isEqualToIgnoringWhitespace(
            """
            {"o": {"x": 10}}
            """
        );
        assertThat(doc.doc().getFields("o.x")).isEmpty();
        assertThat(doc.doc().getFields("o.y")).isEmpty();
        assertThat(doc.doc().getFields())
            .as("source, seqNo, id...")
            .hasSize(6);
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
        IndexItem item = item(value);
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);
        DocTableInfo actualTable = addColumns(executor, table, newColumns);
        indexer.updateTargets(actualTable::getReference);
        ParsedDocument parsedDoc = indexer.index(item);

        assertThat(newColumns)
            .satisfiesExactly(
                col1 -> assertThat(col1)
                    .isReference()
                    .hasName("o['xs']")
                    .hasType(new ArrayType<>(DataTypes.LONG))
            );

        assertThat(source(parsedDoc, actualTable)).isIn(
            "{\"o\":{\"x\":10,\"xs\":[2,3,4]}}",
            "{\"o\":{\"xs\":[2,3,4],\"x\":10}}"
        );

        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);
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
        assertThat(source(parsedDoc, table))
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
        assertThat(source(parsedDoc, table)).isEqualTo(
            "{\"x\":10,\"y\":0}"
        );
        assertThat(parsedDoc.doc().getFields())
            .hasSize(8);
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
        assertThat(source(parsedDoc, table)).isEqualTo(
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
        assertThat(source(parsedDoc, table)).isEqualTo(
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
        assertThat(source(parsedDoc, table)).isEqualTo(
            "{\"o\":{\"x\":0,\"y\":2,\"z\":20}}"
        );
    }

    @Test
    @Ignore("https://github.com/crate/crate/issues/14189")
    /*
     * This isolated test would pass without the validation in {@link AnalyzedColumnDefintion} since it covers only
     * part of code path but actually running a {@code CREATE TABLE tbl (x int, o object as (x int) default {x=10})}
     * throws:
     *    MapperParsingException[Failed to parse mapping: Mapping definition for [o] has unsupported
     *    parameters:  [default_expr : {"x"=10}]]}
     */
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
        assertThat(source(parsedDoc, table)).isEqualToIgnoringWhitespace("""
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
        assertThat(source(parsedDoc, table)).isEqualToIgnoringWhitespace("""
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
            .hasMessage("Failed CONSTRAINT c1 CHECK (\"x\" > 10) for values: [8, 10]");

        ParsedDocument parsedDoc = indexer.index(item(20, null));
        assertThat(source(parsedDoc, table)).isEqualToIgnoringWhitespace("""
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
        assertThatThrownBy(() -> indexer.collectSchemaUpdates(item(Map.of("x", 10, "y", 20))))
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
        List<Reference> newColumns = indexer.collectSchemaUpdates(item(Map.of("x", 10, "y", 20)));
        assertThat(newColumns).satisfiesExactly(
            r -> assertThat(r)
                .isReference()
                .hasName("o['y']")
                .hasType(DataTypes.LONG)
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
        assertThat(source(doc, table)).isEqualTo(
            "{\"o\":{}}"
        );
    }

    @Test
    public void test_indexing_float_results_in_float_field() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x float)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        var ref = table.getReference(new ColumnIdent("x"));

        Indexer indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "x");
        ParsedDocument doc = indexer.index(item(42.2f));
        IndexableField[] fields = doc.doc().getFields(ref.storageIdent());
        assertThat(fields).satisfiesExactly(
            x -> assertThat(x)
                .isExactlyInstanceOf(FloatField.class)
                .extracting("fieldsData")
                .isEqualTo((long) NumericUtils.floatToSortableInt(42.2f))
        );
    }

    @Test
    public void test_can_index_fulltext_column() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x text index using fulltext with (analyzer = 'english'))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        var ref = table.getReference(new ColumnIdent("x"));

        var indexer = getIndexer(e, "tbl", TextFieldMapper.Defaults.FIELD_TYPE, "x");
        ParsedDocument doc = indexer.index(item("Hello World"));
        IndexableField[] fields = doc.doc().getFields(ref.storageIdent());
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
                    new BitStringType(1),
                    FloatVectorType.INSTANCE_ONE
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

            MapperService mapperService = indexEnv.mapperService();
            Indexer indexer = new Indexer(
                table.ident().indexNameOrAlias(),
                table,
                new CoordinatorTxnCtx(e.getSessionSettings()),
                e.nodeCtx,
                mapperService::getLuceneFieldType,
                Lists2.map(columns, c -> table.getReference(c)),
                null
            );
            ParsedDocument doc = indexer.index(item(values.toArray()));
            Map<String, Object> source = sourceMap(doc, table);
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
        IndexItem item = item(42, "Hello", 21);
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);
        ParsedDocument doc = indexer.index(item);
        assertThat(newColumns).satisfiesExactly(
            x -> assertThat(x)
                .isReference()
                .hasName("y")
                .hasType(DataTypes.STRING)
                .hasPosition(-1),
            x -> assertThat(x)
                .isReference()
                .hasName("z")
                .hasType(DataTypes.LONG)
                .hasPosition(-2)
        );
        assertThat(source(doc, table)).isEqualToIgnoringWhitespace(
            """
            {"x": 42, "y": "Hello", "z": 21}
            """
        );

        newColumns = indexer.collectSchemaUpdates(item(42, "Hello", 22));
        assertThat(newColumns)
            .as("Doesn't repeatedly add new column")
            .hasSize(0);
    }

    @Test
    public void test_cannot_add_dynamic_column_on_strict_table() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        assertThatThrownBy(() -> {
            new Indexer(
                table.ident().indexNameOrAlias(),
                table,
                new CoordinatorTxnCtx(e.getSessionSettings()),
                e.nodeCtx,
                column -> NumberFieldMapper.FIELD_TYPE,
                List.<Reference>of(
                    new DynamicReference(
                        new ReferenceIdent(table.ident(), "y"),
                        RowGranularity.DOC,
                        -1
                    )
                ),
                null
            );
        }).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot add column `y` to table `doc.tbl` with column policy `strict`");
    }

    @Test
    public void test_source_includes_null_values_in_arrays() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (xs int[])")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        var indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "xs");
        ParsedDocument doc = indexer.index(item(Arrays.asList(1, 42, null, 21)));
        assertThat(source(doc, table)).isEqualToIgnoringWhitespace(
            """
            {"xs": [1, 42, null, 21]}
            """
        );
    }

    @Test
    public void test_can_have_ft_index_for_array() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (xs text[], index ft using fulltext (xs))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        var refFt = table.indexColumn(new ColumnIdent("ft"));
        var indexer = getIndexer(e, "tbl", KeywordFieldMapper.Defaults.FIELD_TYPE, "xs");
        ParsedDocument doc = indexer.index(item(List.of("foo", "bar", "baz")));
        assertThat(doc.doc().getFields(refFt.storageIdent())).hasSize(3);
        assertThat(source(doc, table)).isEqualToIgnoringWhitespace(
            """
            {"xs": ["foo", "bar", "baz"]}
            """
        );
    }

    @Test
    public void test_empty_array_and_array_with_nulls_does_not_result_in_new_column() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object (dynamic)) with (column_policy = 'dynamic')")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            e.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("o")),
                table.getDynamic(new ColumnIdent("n1"), true, false),
                table.getDynamic(new ColumnIdent("n2"), true, false)
            ),
            null
        );
        List<Object> n1 = List.of();
        List<Object> n2 = Arrays.asList(null, null);
        IndexItem item = item(Map.of("inner", n1), n1, n2);
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);
        ParsedDocument doc = indexer.index(item);
        assertThat(newColumns).isEmpty();
        assertThat(source(doc, table)).isIn(
            "{\"o\":{\"inner\":[]},\"n1\":[],\"n2\":[null,null]}",
            "{\"n1\":[],\"n2\":[null,null],\"o\":{\"inner\":[]}}"
        );
    }

    @Test
    public void test_leaves_out_generated_column_if_dependency_is_null() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int generated always as x + 1)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "x");
        IndexItem item = item(new Object[] { null });
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);
        ParsedDocument doc = indexer.index(item);
        assertThat(newColumns).isEmpty();
        assertThat(doc.source().utf8ToString()).isEqualTo("{}");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_adds_non_deterministic_defaults_and_generated_columns() throws Exception {
        long now = System.currentTimeMillis();
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("""
                create table tbl (
                    o object as (
                        x int generated always as random(),
                        y int
                    ),
                    z timestamp default now()
                )
                """)
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "o");
        IndexItem item = item(MapBuilder.newMapBuilder().put("y", 2).map());
        ParsedDocument doc = indexer.index(item);
        Map<String, Object> source = sourceMap(doc, table);
        assertThat(source).containsKeys("o", "z");
        assertThat((Map<String, ?>) source.get("o")).containsKeys("x", "y");

        assertThat(indexer.hasUndeterministicSynthetics()).isTrue();
        Object[] insertValues = indexer.addGeneratedValues(item);
        assertThat(insertValues).hasSize(2);
        assertThat((Map<String, ?>) insertValues[0]).containsKeys("x", "y");
        assertThat((long) insertValues[1]).isGreaterThanOrEqualTo(now);
    }

    @Test
    public void test_fields_order_in_source_is_determinisitc() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, o object, y int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        Indexer indexer = getIndexer(e, "tbl", NumberFieldMapper.FIELD_TYPE, "x", "o", "y");
        BytesReference source = null;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(4, 7); i++) {
            keys.add(randomAlphaOfLength(randomIntBetween(4, 20)));
        }
        List<Reference> newColumns = null;
        for (int i = 0; i < 10; i++) {
            Map<String, Integer> o = new LinkedHashMap<>();
            for (int c = 0; c < keys.size(); c++) {
                String key = keys.get(c);
                o.put(key, c);
            }
            IndexItem item = item(10, o, 50);
            List<Reference> collectedNewColumns = indexer.collectSchemaUpdates(item);

            if (collectedNewColumns.isEmpty() == false) {
                DocTableInfo actualTable = addColumns(e, table, collectedNewColumns);
                indexer.updateTargets(actualTable::getReference);
            }
            ParsedDocument doc = indexer.index(item);
            if (source == null) {
                source = doc.source();
                newColumns = collectedNewColumns;
                logger.info("Dynamic column order: {}", newColumns);
                logger.info("New keys order: {}", keys);
            } else {
                assertThat(doc.source())
                    .as("fields in " + doc.source().utf8ToString() + " must have same order in " + source.utf8ToString())
                    .isEqualTo(source);

            }
        }

        DocTableInfo newTable = addColumns(e, table, newColumns);
        Reference oRef = newTable.getReference(new ColumnIdent("o"));
        assertThat(((ObjectType) oRef.valueType()).innerTypes().keySet()).containsExactlyElementsOf(keys);
        indexer = new Indexer(
            newTable.ident().indexNameOrAlias(),
            newTable,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            e.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of("x", "o", "y").stream()
                .map(x -> newTable.getReference(new ColumnIdent(x)))
                .toList(),
            null
        );
        Map<String, Integer> o = new LinkedHashMap<>();
        for (int c = 0; c < keys.size(); c++) {
            String key = keys.get(c);
            o.put(key, c);
        }
        IndexItem item = item(10, o, 50);
        List<Reference> collectedNewColumns = indexer.collectSchemaUpdates(item);
        assertThat(collectedNewColumns).isEmpty();
        ParsedDocument doc = indexer.index(item);
        assertThat(doc.source())
            .as(
                "Fields in new source expected to match old source\n" +
                "old=" + doc.source().utf8ToString() + "\n" +
                "new=" + source.utf8ToString() + "\n" +
                "keys=" + keys)
            .isEqualTo(source);
    }

    /**
     * Ensures that docs containing numeric values created by our indexer uses the same Lucene fields definition
     * than the FieldMapper used when inserting documents read from the translog.
     */
    @Test
    public void test_indexing_number_results_in_same_fields_as_document_mapper_if_not_indexed() throws Exception {
        var idx = 0;
        for (var dt : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            var tableName = "tbl_" + idx++;
            SQLExecutor e = SQLExecutor.builder(clusterService)
                    .addTable("create table " + tableName + " (x " + dt.getName() + " INDEX OFF)")
                    .build();

            Indexer indexer = getIndexer(e, tableName, NumberFieldMapper.FIELD_TYPE, "x");
            ParsedDocument doc = indexer.index(item(1));
            IndexableField[] fields = doc.doc().getFields("x");

            // @formatter: off
            String mapping = Strings.toString(JsonXContent.builder()
                .startObject()
                    .startObject("properties")
                        .startObject("x")
                            .field("type", DataTypes.esMappingNameFrom(dt.id()))
                            .field("index", false)
                        .endObject()
                    .endObject()
                .endObject());

            var indexName = e.resolveTableInfo(tableName).ident().indexNameOrAlias();
            DocumentMapper mapper = mapper(indexName, mapping);
            ParsedDocument docFromSource = mapper.parse(
                    new SourceToParse(indexName, "dummy-id-1", doc.source(), XContentType.JSON)
            );
            IndexableField[] fieldsFromSource = docFromSource.doc().getFields("x");

            assertThat(fields.length).isEqualTo(fieldsFromSource.length);
            for (int i = 0; i < fields.length; i++) {
                assertThat(fields[i].toString()).isEqualTo(fieldsFromSource[i].toString());
            }
        }
    }

    @Test
    public void test_indexing_ip_results_in_same_fields_as_document_mapper_if_not_indexed() throws Exception {
        var idx = 0;
        var tableName = "tbl";
        var dt = IpType.INSTANCE;
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable("create table " + tableName + " (x " + dt.getName() + " INDEX OFF)")
                .build();

        Indexer indexer = getIndexer(e, tableName, NumberFieldMapper.FIELD_TYPE, "x");

        ParsedDocument doc = indexer.index(item("127.0.0.1"));
        IndexableField[] fields = doc.doc().getFields("x");

        // @formatter: off
        String mapping = Strings.toString(JsonXContent.builder()
            .startObject()
                .startObject("properties")
                    .startObject("x")
                        .field("type", DataTypes.esMappingNameFrom(dt.id()))
                        .field("index", false)
                    .endObject()
                .endObject()
            .endObject());

        var indexName = e.resolveTableInfo(tableName).ident().indexNameOrAlias();
        DocumentMapper mapper = mapper(indexName, mapping);
        ParsedDocument docFromSource = mapper.parse(
                new SourceToParse(indexName, "dummy-id-1", doc.source(), XContentType.JSON)
        );
        IndexableField[] fieldsFromSource = docFromSource.doc().getFields("x");

        assertThat(fields.length).isEqualTo(fieldsFromSource.length);
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].toString()).isEqualTo(fieldsFromSource[i].toString());
        }
    }

    @Test
    public void test_indexing_bitstring_results_in_same_fields_as_document_mapper_if_not_indexed() throws Exception {
        var idx = 0;
        var tableName = "tbl";
        var dt = BitStringType.INSTANCE_ONE;
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable("create table " + tableName + " (x " + dt.getName() + "(1) INDEX OFF)")
                .build();

        Indexer indexer = getIndexer(e, tableName, NumberFieldMapper.FIELD_TYPE, "x");

        ParsedDocument doc = indexer.index(item(BitString.ofRawBits("1")));
        IndexableField[] fields = doc.doc().getFields("x");

        // @formatter: off
        String mapping = Strings.toString(JsonXContent.builder()
            .startObject()
                .startObject("properties")
                    .startObject("x")
                        .field("type", DataTypes.esMappingNameFrom(dt.id()))
                        .field("index", false)
                        .field("length", 1)
                    .endObject()
                .endObject()
            .endObject());

        var indexName = e.resolveTableInfo(tableName).ident().indexNameOrAlias();
        DocumentMapper mapper = mapper(indexName, mapping);
        ParsedDocument docFromSource = mapper.parse(
                new SourceToParse(indexName, "dummy-id-1", doc.source(), XContentType.JSON)
        );
        IndexableField[] fieldsFromSource = docFromSource.doc().getFields("x");

        assertThat(fields.length).isEqualTo(fieldsFromSource.length);
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].toString()).isEqualTo(fieldsFromSource[i].toString());
        }
    }

    @Test
    public void test_indexing_boolean_results_in_same_fields_as_document_mapper_if_not_indexed() throws Exception {
        var idx = 0;
        var tableName = "tbl";
        var dt = BooleanType.INSTANCE;
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable("create table " + tableName + " (x " + dt.getName() + " INDEX OFF)")
                .build();

        Indexer indexer = getIndexer(e, tableName, NumberFieldMapper.FIELD_TYPE, "x");

        ParsedDocument doc = indexer.index(item(true));
        IndexableField[] fields = doc.doc().getFields("x");

        // @formatter: off
        String mapping = Strings.toString(JsonXContent.builder()
            .startObject()
                .startObject("properties")
                    .startObject("x")
                        .field("type", DataTypes.esMappingNameFrom(dt.id()))
                        .field("index", false)
                    .endObject()
                .endObject()
            .endObject());

        var indexName = e.resolveTableInfo(tableName).ident().indexNameOrAlias();
        DocumentMapper mapper = mapper(indexName, mapping);
        ParsedDocument docFromSource = mapper.parse(
                new SourceToParse(indexName, "dummy-id-1", doc.source(), XContentType.JSON)
        );
        IndexableField[] fieldsFromSource = docFromSource.doc().getFields("x");

        assertThat(fields.length).isEqualTo(fieldsFromSource.length);
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].toString()).isEqualTo(fieldsFromSource[i].toString());
        }
    }

    @Test
    public void test_index_nested_array() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int) with (column_policy = 'dynamic')")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Reference y = new DynamicReference(new ReferenceIdent(table.ident(), "y"), RowGranularity.DOC, 2);
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x, y),
            null
        );
        IndexItem item = item(10, List.of(List.of(1, 2), List.of(3, 4)));
        List<Reference> newColumns = indexer.collectSchemaUpdates(item);
        ParsedDocument doc = indexer.index(item);
        assertThat(newColumns).satisfiesExactly(
            column -> assertThat(column)
                .isReference()
                .hasName("y")
                .hasType(new ArrayType<>(new ArrayType<>(DataTypes.LONG)))
        );
        assertThat(source(doc, table)).isEqualTo("""
            {"x":10,"y":[[1,2],[3,4]]}"""
        );
    }

    @Test
    public void test_generated_column_can_refer_to_a_non_string_partitioned_by_column() throws Exception {
        String partition = new PartitionName(new RelationName("doc", "t"), List.of("2")).asIndexName();
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addPartitionedTable("""
             CREATE TABLE t (
                 a INT,
                 parted INT CHECK (parted > 1),
                 gen_from_parted INT as parted + 1
             ) PARTITIONED BY (parted)
             """
            ).build();
        DocTableInfo table = executor.resolveTableInfo("t");
        Indexer indexer = new Indexer(
            partition,
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(
                table.getReference(new ColumnIdent("a"))
                // 'Parted' is not in targets to imitate insert-from-subquery behavior
                //  which excludes partitioned columns from targets
            ),
            null
        );

        // Imitating problematic query
        // insert into t (a, parted) select 1, 2
        // We are inserting into partition 2, so b = 2.
        ParsedDocument parsedDoc = indexer.index(item(1));
        assertThat(source(parsedDoc, table)).isEqualToIgnoringWhitespace(
            """
            {"a":1, "gen_from_parted": 3}
            """
        );
    }

    @Test
    public void test_check_constraint_on_object_sub_column_is_verified() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (obj object as (x int check (obj['x'] > 10)))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("obj"));
        Indexer indexer = new Indexer(
            table.ident().indexNameOrAlias(),
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x),
            null
        );
        assertThatThrownBy(() -> indexer.index(item(Map.of("x", 5))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContainingAll("Failed CONSTRAINT", "CHECK (\"obj\"['x'] > 10) for values: [{x=5}]");
    }

    @Test
    public void test_empty_arrays_are_prefixed_as_unknown() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable("create table tbl (i int) with (column_policy='dynamic')")
                .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        var indexer = getIndexer(e, "tbl", null, "empty_arr");
        ParsedDocument doc = indexer.index(item(List.of()));
        assertThat(doc.source().utf8ToString()).isEqualToIgnoringWhitespace(
                """
                {"_u_empty_arr":[]}
                """
        );
        // prefix is stripped on non _raw lookups
        assertThat(source(doc, table)).isEqualToIgnoringWhitespace(
                """
                {"empty_arr":[]}
                """
        );
    }

    @Test
    public void test_empty_arrays_are_not_prefixed_as_unknown_on_tables_created_less_5_5() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable(
                        "create table tbl (i int) with (column_policy='dynamic')",
                        Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_4_0).build()
                )
                .build();

        var indexer = getIndexer(e, "tbl", null, "empty_arr");
        ParsedDocument doc = indexer.index(item(List.of()));
        assertThat(doc.source().utf8ToString()).isEqualToIgnoringWhitespace(
                """
                {"empty_arr":[]}
                """
        );
    }

    @UseNewCluster
    @Test
    public void test_ignored_object_child_columns_are_prefixed() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
                .addTable("create table tbl (o object (ignored) as (i int))")
                .build();
        DocTableInfo table = e.resolveTableInfo("tbl");

        var indexer = getIndexer(e, "tbl", null, "o");
        ParsedDocument doc = indexer.index(item(Map.of("i", 1, "ignored_col", "foo")));
        assertThat(doc.source().utf8ToString()).isEqualToIgnoringWhitespace(
                """
                {"1":{"2":1,"_u_ignored_col":"foo"}}
                """
        );
        // prefix is stripped and OID's replaced on non _raw lookups
        assertThat(source(doc, table)).isIn(
                "{\"o\":{\"i\":1,\"ignored_col\":\"foo\"}}",
                "{\"o\":{\"ignored_col\":\"foo\",\"i\":1}}"
        );
    }

    @Test
    public void test_ignored_object_child_columns_are_not_prefixed_on_tables_created_less_5_5() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
                // old tables created with CrateDB < 5.5.0 do not assign any OID, fake it here
                .setColumnOidSupplier(() -> COLUMN_OID_UNASSIGNED)
                .addTable("create table tbl (o object (ignored) as (i int))",
                        Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_4_0).build()
                )
                .build();

        var indexer = getIndexer(e, "tbl", null, "o");
        ParsedDocument doc = indexer.index(item(Map.of("i", 1, "ignored_col", "foo")));
        assertThat(doc.source().utf8ToString()).isEqualToIgnoringWhitespace(
                """
                {"o":{"i":1,"ignored_col":"foo"}}
                """
        );
    }
}
