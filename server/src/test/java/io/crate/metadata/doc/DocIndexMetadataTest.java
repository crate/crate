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

package io.crate.metadata.doc;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.junit.Before;
import org.junit.Test;

import io.crate.Constants;
import io.crate.action.sql.Cursors;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.CreateTableStatementAnalyzer;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ParamTypeHints;
import io.crate.data.Row;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.planner.operators.SubQueryResults;
import io.crate.server.xcontent.XContentHelper;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Statement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

// @formatter:off
public class DocIndexMetadataTest extends CrateDummyClusterServiceUnitTest {

    private NodeContext nodeCtx;

    private IndexMetadata getIndexMetadata(String indexName,
                                           XContentBuilder builder) throws IOException {
        Map<String, Object> mappingSource = XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).map();
        mappingSource = sortProperties(mappingSource);

        Settings.Builder settingsBuilder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", org.elasticsearch.Version.CURRENT);

        IndexMetadata.Builder mdBuilder = IndexMetadata.builder(indexName)
            .settings(settingsBuilder)
            .putMapping(new MappingMetadata(mappingSource));
        return mdBuilder.build();
    }

    private DocTableInfo newTable(IndexMetadata metadata, String name) throws IOException {
        RelationName relation = new RelationName(Schemas.DOC_SCHEMA_NAME, name);
        DocIndexMetadata docIndexMetadata = new DocIndexMetadata(
            nodeCtx,
            metadata,
            relation,
            null
        ).build();
        return new DocTableInfo(
            relation,
            docIndexMetadata.references(),
            docIndexMetadata.notNullColumns(),
            docIndexMetadata.indices(),
            docIndexMetadata.analyzers(),
            docIndexMetadata.pkConstraintName(),
            docIndexMetadata.primaryKey(),
            docIndexMetadata.checkConstraints(),
            docIndexMetadata.routingCol(),
            docIndexMetadata.hasAutoGeneratedPrimaryKey(),
            new String [] { relation.indexNameOrAlias() },
            new String [] { relation.indexNameOrAlias() },
            docIndexMetadata.numberOfShards(),
            docIndexMetadata.numberOfReplicas(),
            docIndexMetadata.tableParameters(),
            docIndexMetadata.partitionedBy(),
            List.of(),
            docIndexMetadata.columnPolicy(),
            docIndexMetadata.versionCreated(),
            docIndexMetadata.versionUpgraded(),
            docIndexMetadata.isClosed(),
            docIndexMetadata.supportedOperations()
        );
    }

    @Before
    public void setupNodeCtx() {
        nodeCtx = createNodeContext();
    }

    @Test
    public void testNestedColumnIdent() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject("properties")
                    .startObject("person")
                    .field("position", 1)
                        .startObject("properties")
                            .startObject("addresses")
                            .field("position", 2)
                                .startObject("properties")
                                    .startObject("city")
                                        .field("type", "string")
                                        .field("position", 3)
                                    .endObject()
                                    .startObject("country")
                                        .field("type", "string")
                                        .field("position", 4)
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        Reference reference = table.getReference(new ColumnIdent("person", Arrays.asList("addresses", "city")));
        assertThat(reference).isNotNull();
    }

    @Test
    public void testExtractObjectColumnDefinitions() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("properties")
            .startObject("implicit_dynamic")
            .field("position", 1)
            .startObject("properties")
            .startObject("name")
            .field("type", "string")
            .field("position", 5)
            .endObject()
            .endObject()
            .endObject()
            .startObject("explicit_dynamic")
            .field("position", 2)
            .field("dynamic", "true")
            .startObject("properties")
            .startObject("name")
            .field("type", "string")
            .field("position", 6)
            .endObject()
            .startObject("age")
            .field("type", "integer")
            .field("position", 7)
            .endObject()
            .endObject()
            .endObject()
            .startObject("ignored")
            .field("position", 3)
            .field("dynamic", "false")
            .startObject("properties")
            .startObject("name")
            .field("type", "string")
            .field("position", 8)
            .endObject()
            .startObject("age")
            .field("type", "integer")
            .field("position", 9)
            .endObject()
            .endObject()
            .endObject()
            .startObject("strict")
            .field("position", 4)
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("age")
            .field("type", "integer")
            .field("position", 10)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");
        assertThat(table.columns()).hasSize(4);
        assertThat(table).hasSize(20);
        assertThat(table.getReference(new ColumnIdent("implicit_dynamic")).columnPolicy()).isEqualTo(ColumnPolicy.DYNAMIC);
        assertThat(table.getReference(new ColumnIdent("explicit_dynamic")).columnPolicy()).isEqualTo(ColumnPolicy.DYNAMIC);
        assertThat(table.getReference(new ColumnIdent("ignored")).columnPolicy()).isEqualTo(ColumnPolicy.IGNORED);
        assertThat(table.getReference(new ColumnIdent("strict")).columnPolicy()).isEqualTo(ColumnPolicy.STRICT);
    }

    @Test
    public void testExtractColumnDefinitions() throws Exception {
        // @formatter:off
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject("_meta")
                    .field("primary_keys", "integerIndexed")
                .endObject()
                .startObject("properties")
                    .startObject("integerIndexed")
                        .field("type", "integer")
                        .field("position", 1)
                    .endObject()
                    .startObject("integerIndexedBWC")
                        .field("type", "integer")
                        .field("position", 2)
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("integerNotIndexed")
                        .field("type", "integer")
                        .field("position", 3)
                        .field("index", "false")
                    .endObject()
                    .startObject("integerNotIndexedBWC")
                        .field("type", "integer")
                        .field("position", 4)
                        .field("index", "no")
                    .endObject()
                    .startObject("stringNotIndexed")
                        .field("type", "string")
                        .field("position", 5)
                        .field("index", "false")
                    .endObject()
                    .startObject("stringNotIndexedBWC")
                        .field("type", "string")
                        .field("position", 6)
                        .field("index", "no")
                    .endObject()
                    .startObject("stringNotAnalyzed")
                        .field("type", "keyword")
                        .field("position", 7)
                    .endObject()
                    .startObject("stringNotAnalyzedBWC")
                        .field("type", "string")
                        .field("position", 8)
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("stringAnalyzed")
                        .field("type", "text")
                        .field("position", 9)
                        .field("analyzer", "standard")
                    .endObject()
                    .startObject("stringAnalyzedBWC")
                        .field("type", "string")
                        .field("position", 10)
                        .field("index", "analyzed")
                        .field("analyzer", "standard")
                    .endObject()
                    .startObject("person").field("position", 11)
                        .startObject("properties")
                            .startObject("first_name")
                                .field("type", "string")
                                .field("position", 12)
                            .endObject()
                            .startObject("birthday")
                                .field("type", "date")
                                .field("position", 13)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        // @formatter:on

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        assertThat(table.columns()).hasSize(11);
        assertThat(table).hasSize(23);

        Reference birthday = table.getReference(new ColumnIdent("person", "birthday"));
        assertThat(birthday.valueType()).isEqualTo(DataTypes.TIMESTAMPZ);
        assertThat(birthday.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(birthday.defaultExpression()).isNull();

        Reference integerIndexed = table.getReference(new ColumnIdent("integerIndexed"));
        assertThat(integerIndexed.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(integerIndexed.defaultExpression()).isNull();

        Reference integerIndexedBWC = table.getReference(new ColumnIdent("integerIndexedBWC"));
        assertThat(integerIndexedBWC.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(integerIndexedBWC.defaultExpression()).isNull();

        Reference integerNotIndexed = table.getReference(new ColumnIdent("integerNotIndexed"));
        assertThat(integerNotIndexed.indexType()).isEqualTo(IndexType.NONE);
        assertThat(integerNotIndexed.defaultExpression()).isNull();

        Reference integerNotIndexedBWC = table.getReference(new ColumnIdent("integerNotIndexedBWC"));
        assertThat(integerNotIndexedBWC.indexType()).isEqualTo(IndexType.NONE);
        assertThat(integerNotIndexedBWC.defaultExpression()).isNull();

        Reference stringNotIndexed = table.getReference(new ColumnIdent("stringNotIndexed"));
        assertThat(stringNotIndexed.indexType()).isEqualTo(IndexType.NONE);
        assertThat(stringNotIndexed.defaultExpression()).isNull();

        Reference stringNotIndexedBWC = table.getReference(new ColumnIdent("stringNotIndexedBWC"));
        assertThat(stringNotIndexedBWC.indexType()).isEqualTo(IndexType.NONE);
        assertThat(stringNotIndexedBWC.defaultExpression()).isNull();

        Reference stringNotAnalyzed = table.getReference(new ColumnIdent("stringNotAnalyzed"));
        assertThat(stringNotAnalyzed.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(stringNotAnalyzed.defaultExpression()).isNull();

        Reference stringNotAnalyzedBWC = table.getReference(new ColumnIdent("stringNotAnalyzedBWC"));
        assertThat(stringNotAnalyzedBWC.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(stringNotAnalyzedBWC.defaultExpression()).isNull();

        Reference stringAnalyzed = table.getReference(new ColumnIdent("stringAnalyzed"));
        assertThat(stringAnalyzed.indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(stringAnalyzed.defaultExpression()).isNull();

        Reference stringAnalyzedBWC = table.getReference(new ColumnIdent("stringAnalyzedBWC"));
        assertThat(stringAnalyzedBWC.indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(stringAnalyzedBWC.defaultExpression()).isNull();

        List<String> allRefNames = StreamSupport.stream(table.spliterator(), false)
            .map(r -> r.column().fqn())
            .toList();
        assertThat(allRefNames).containsExactlyInAnyOrder(
            "_doc", "_fetchid", "_id", "_raw", "_score", "_uid", "_version", "_docid", "_seq_no",
            "_primary_term", "integerIndexed", "integerIndexedBWC", "integerNotIndexed", "integerNotIndexedBWC",
            "person", "person.birthday", "person.first_name",
            "stringAnalyzed", "stringAnalyzedBWC", "stringNotAnalyzed", "stringNotAnalyzedBWC",
            "stringNotIndexed", "stringNotIndexedBWC");
    }

    @Test
    public void testExtractColumnDefinitionsWithDefaultExpression() throws Exception {
        // @formatter:off
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject("_meta")
                    .field("primary_keys", "integerIndexed")
                .endObject()
                .startObject("properties")
                    .startObject("integerIndexed")
                        .field("type", "integer")
                        .field("position", 1)
                        .field("default_expr", "1")
                    .endObject()
                    .startObject("integerNotIndexed")
                        .field("type", "integer")
                        .field("position", 2)
                        .field("index", "false")
                        .field("default_expr", "1")
                    .endObject()
                    .startObject("stringNotIndexed")
                        .field("type", "string")
                        .field("position", 3)
                        .field("index", "false")
                        .field("default_expr", "'default'")
                    .endObject()
                    .startObject("stringNotAnalyzed")
                        .field("type", "keyword")
                        .field("position", 4)
                        .field("default_expr", "'default'")
                    .endObject()
                    .startObject("stringAnalyzed")
                        .field("type", "text")
                        .field("position", 5)
                        .field("analyzer", "standard")
                        .field("default_expr", "'default'")
                    .endObject()

                    .startObject("birthday")
                        .field("type", "date")
                        .field("position", 6)
                        .field("default_expr", "current_timestamp(3)")
                    .endObject()

                    .startObject("integerWithCast")
                        .field("type", "integer")
                        .field("position", 7)
                        .field("default_expr", "2::long * 5::long")
                    .endObject()
                .endObject()
            .endObject();
        // @formatter:on

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        assertThat(table.columns()).hasSize(7);
        assertThat(table).hasSize(17);

        Reference birthday = table.getReference(new ColumnIdent("birthday"));
        assertThat(birthday.valueType()).isEqualTo(DataTypes.TIMESTAMPZ);
        assertThat(birthday.defaultExpression())
            .isFunction("current_timestamp", List.of(DataTypes.INTEGER));

        Reference integerIndexed = table.getReference(new ColumnIdent("integerIndexed"));
        assertThat(integerIndexed.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(integerIndexed.defaultExpression()).isLiteral(1);


        Reference integerNotIndexed = table.getReference(new ColumnIdent("integerNotIndexed"));
        assertThat(integerNotIndexed.indexType()).isEqualTo(IndexType.NONE);
        assertThat(integerNotIndexed.defaultExpression()).isLiteral(1);

        Reference stringNotIndexed = table.getReference(new ColumnIdent("stringNotIndexed"));
        assertThat(stringNotIndexed.indexType()).isEqualTo(IndexType.NONE);
        assertThat(stringNotIndexed.defaultExpression()).isLiteral("default");

        Reference stringNotAnalyzed = table.getReference(new ColumnIdent("stringNotAnalyzed"));
        assertThat(stringNotAnalyzed.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(stringNotAnalyzed.defaultExpression()).isLiteral("default");

        Reference stringAnalyzed = table.getReference(new ColumnIdent("stringAnalyzed"));
        assertThat(stringAnalyzed.indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(stringAnalyzed.defaultExpression()).isLiteral("default");

        Reference integerWithCast = table.getReference(new ColumnIdent("integerWithCast"));
        assertThat(integerWithCast.indexType()).isEqualTo(IndexType.PLAIN);
        assertThat(integerWithCast.defaultExpression().valueType()).isEqualTo(DataTypes.INTEGER);
        assertThat(integerWithCast.defaultExpression().symbolType()).isEqualTo(SymbolType.FUNCTION);
        assertThat(((Function) integerWithCast.defaultExpression()).name()).isEqualTo(ImplicitCastFunction.NAME);

        List<String> allRefNames = StreamSupport.stream(table.spliterator(), false)
            .map(r -> r.column().fqn())
            .toList();
        assertThat(allRefNames).containsExactlyInAnyOrder(
            "_raw", "_doc", "_seq_no", "_version", "_id", "_uid",
            "_score", "_fetchid", "_primary_term", "_docid",
            "birthday", "integerIndexed", "integerNotIndexed", "integerWithCast",
            "stringAnalyzed", "stringNotAnalyzed", "stringNotIndexed");
    }

    @Test
    public void testExtractPartitionedByColumns() throws Exception {
        // @formatter:off
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("_meta")
                .field("primary_keys", "id")
                .startArray("partitioned_by")
                    .startArray()
                    .value("datum").value("date")
                    .endArray()
                .endArray()
            .endObject()
            .startObject("properties")
                .startObject("id")
                    .field("type", "integer")
                    .field("position", 1)
                .endObject()
                .startObject("title")
                    .field("type", "string")
                    .field("position", 2)
                    .field("index", "false")
                .endObject()
                .startObject("datum")
                    .field("type", "date")
                    .field("position", 3)
                .endObject()
                .startObject("content")
                    .field("type", "string")
                    .field("position", 4)
                    .field("index", "true")
                    .field("analyzer", "standard")
                .endObject()
                .startObject("person")
                .field("position", 5)
                    .startObject("properties")
                        .startObject("first_name")
                            .field("type", "string")
                            .field("position", 7)
                        .endObject()
                        .startObject("birthday")
                            .field("type", "date")
                            .field("position", 8)
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("nested")
                .field("position", 6)
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("inner_nested")
                            .field("type", "date")
                            .field("position", 9)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject();
        // @formatter:on
        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        assertThat(table.columns()).hasSize(6);
        assertThat(table).hasSize(19);
        assertThat(table.partitionedByColumns()).hasSize(1);
        assertThat(table.partitionedByColumns().get(0).valueType()).isEqualTo(DataTypes.TIMESTAMPZ);
        assertThat(table.partitionedByColumns().get(0).column().fqn()).isEqualTo("datum");

        assertThat(table.partitionedBy()).hasSize(1);
        assertThat(table.partitionedBy().get(0)).isEqualTo(ColumnIdent.fromPath("datum"));
    }

    @Test
    public void testExtractPartitionedByWithPartitionedByInColumns() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("_meta")
            .startArray("partitioned_by")
            .startArray()
            .value("datum").value("date")
            .endArray()
            .endArray()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("datum")
            .field("type", "date")
            .field("position", 2)
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        // partitioned by column is not added twice
        assertThat(table.columns()).hasSize(2);
        assertThat(table).hasSize(12);
        assertThat(table.partitionedByColumns()).hasSize(1);
    }

    @Test
    public void testExtractPartitionedByWithNestedPartitionedByInColumns() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("_meta")
            .startArray("partitioned_by")
            .startArray()
            .value("nested.datum").value("date")
            .endArray()
            .endArray()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("nested")
            .field("position", 2)
            .field("type", "nested")
            .startObject("properties")
            .startObject("datum")
            .field("type", "date")
            .field("position", 3)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        // partitioned by column is not added twice
        assertThat(table.columns()).hasSize(2);
        assertThat(table).hasSize(13);
        assertThat(table.partitionedByColumns()).hasSize(1);
    }

    private Map<String, Object> sortProperties(Map<String, Object> mappingSource) {
        return sortProperties(mappingSource, false);
    }

    /**
     * in the DocumentMapper that ES uses at some place the properties of the mapping are sorted.
     * this logic doesn't seem to be triggered if the IndexMetadata is created using the
     * IndexMetadata.Builder.
     * <p/>
     * in order to have the same behaviour as if a Node was started and a index with mapping was created
     * using the ES tools pre-sort the mapping here.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> sortProperties(Map<String, Object> mappingSource, boolean doSort) {
        Map<String, Object> map;
        if (doSort) {
            map = new TreeMap<>();
        } else {
            map = new HashMap<>();
        }

        boolean sortNext;
        Object value;
        for (Map.Entry<String, Object> entry : mappingSource.entrySet()) {
            value = entry.getValue();
            sortNext = entry.getKey().equals("properties");

            if (value instanceof Map) {
                map.put(entry.getKey(), sortProperties((Map<String, Object>) entry.getValue(), sortNext));
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        return map;
    }

    @Test
    public void testExtractColumnDefinitionsFromEmptyIndex() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test2", builder);
        DocTableInfo table = newTable(metadata, "test2");
        assertThat(table.columns()).isEmpty();
    }

    @Test
    public void testDocSysColumnReferences() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("properties")
            .startObject("content")
            .field("type", "string")
            .field("position", 1)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        DocTableInfo table = newTable(getIndexMetadata("test", builder), "test");
        Reference id = table.getReference(new ColumnIdent("_id"));
        assertThat(id).isNotNull();

        Reference version = table.getReference(new ColumnIdent("_version"));
        assertThat(version).isNotNull();

        Reference score = table.getReference(new ColumnIdent("_score"));
        assertThat(score).isNotNull();

        Reference docId = table.getReference(new ColumnIdent("_docid"));
        assertThat(docId).isNotNull();
    }

    @Test
    public void testExtractPrimaryKey() throws Exception {

        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .field("primary_keys", "id")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .startObject("datum")
            .field("type", "date")
            .field("position", 3)
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("position", 4)
            .field("index", "true")
            .field("analyzer", "standard")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test3", builder);
        DocTableInfo table = newTable(metadata, "test3");


        assertThat(table.primaryKey()).containsExactly(new ColumnIdent("id"));

        builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("properties")
            .startObject("content")
            .field("type", "string")
            .field("position", 1)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        table = newTable(getIndexMetadata("test4", builder), "test4");
        assertThat(table.primaryKey()).hasSize(1); // _id is always the fallback primary key

        builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        table = newTable(getIndexMetadata("test5", builder), "test5");
        assertThat(table.primaryKey()).hasSize(1);
    }

    @Test
    public void testExtractMultiplePrimaryKeys() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .array("primary_keys", "id", "title")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test_multi_pk", builder);
        DocTableInfo table = newTable(metadata, "test_multi_pk");
        assertThat(table.primaryKey()).containsExactly(ColumnIdent.fromPath("id"), ColumnIdent.fromPath("title"));
    }

    @Test
    public void testExtractCheckConstraints() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .startObject("check_constraints")
            .field("test3_check_1", "id >= 0")
            .field("test3_check_2", "title != 'Programming Clojure'")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("id").field("type", "integer")
            .field("position", 1).endObject()
            .startObject("title").field("type", "string")
            .field("position", 2).endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test3", builder);
        DocTableInfo table = newTable(metadata, "test3");
        assertThat(table.checkConstraints()).hasSize(2);
        assertThat(table.checkConstraints()
                       .stream()
                       .map(CheckConstraint::expressionStr)
                       .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("id >= 0", "title != 'Programming Clojure'");
    }

    @Test
    public void testExtractNoPrimaryKey() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test_no_pk", builder);
        DocTableInfo table = newTable(metadata, "test_no_pk");
        assertThat(table.primaryKey()).containsExactly(ColumnIdent.fromPath("_id"));

        builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .array("primary_keys") // results in empty list
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        metadata = getIndexMetadata("test_no_pk2", builder);
        table = newTable(metadata, "test_no_pk2");
        assertThat(table.primaryKey()).containsExactly(ColumnIdent.fromPath("_id"));
    }

    @Test
    public void testSchemaWithNotNullColumns() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .startObject("constraints")
            .array("not_null", "id", "title")
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test_notnull_columns", builder);
        DocTableInfo table = newTable(metadata, "test_notnull_columns");

        assertThat(table.columns().stream().map(Reference::isNullable).collect(Collectors.toList())).containsExactly(
            false, false
        );
    }

    @Test
    public void testSchemaWithNotNullNestedColumns() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                    .startObject("_meta")
                        .startObject("constraints")
                            .array("not_null", "nested.level1", "nested.level1.level2")
                        .endObject()
                    .endObject()
                    .startObject("properties")
                        .startObject("nested")
                        .field("position", 1)
                            .field("type", "object")
                                .startObject("properties")
                                    .startObject("level1")
                                    .field("position", 2)
                                        .field("type", "object")
                                        .startObject("properties")
                                            .startObject("level2")
                                                .field("type", "string")
                                                .field("position", 3)
                                        .endObject()
                                    .endObject()
                                 .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test_notnull_columns", builder);
        DocTableInfo table = newTable(metadata, "test_notnull_columns");

        ColumnIdent level1 = new ColumnIdent("nested", "level1");
        ColumnIdent level2 = new ColumnIdent("nested", Arrays.asList("level1", "level2"));
        assertThat(table.notNullColumns()).containsExactlyInAnyOrder(level1, level2);
        assertThat(table.getReference(level1).isNullable()).isFalse();
        assertThat(table.getReference(level2).isNullable()).isFalse();
    }

    @Test
    public void testSchemaWithNotNullGeneratedColumn() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject("_meta")
                    .startObject("generated_columns")
                        .field("week", "date_trunc('week', ts)")
                    .endObject()
                    .startObject("constraints")
                        .array("not_null", "week")
                    .endObject()
                .endObject()
                .startObject("properties")
                    .startObject("ts").field("type", "date")
                    .field("position", 1)
                    .endObject()
                    .startObject("week").field("type", "long")
                    .field("position", 2)
                    .endObject()
                .endObject()
            .endObject();

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        assertThat(table.columns()).hasSize(2);
        Reference week = table.getReference(new ColumnIdent("week"));
        assertThat(week)
            .isNotNull()
            .isExactlyInstanceOf(GeneratedReference.class);
        assertThat(week.isNullable()).isFalse();
        assertThat(((GeneratedReference) week).formattedGeneratedExpression()).isEqualTo("date_trunc('week', ts)");
        assertThat(((GeneratedReference) week).generatedExpression()).isFunction("_cast",
            arg1 -> assertThat(arg1).isFunction("date_trunc", isLiteral("week"), isReference("ts")),
            arg2 -> assertThat(arg2).isLiteral("bigint")
        );
        assertThat(((GeneratedReference) week).referencedReferences()).satisfiesExactly(isReference("ts"));
    }

    @Test
    public void extractRoutingColumn() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .field("primary_keys", "id")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("datum")
            .field("type", "date")
            .field("position", 3)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        DocTableInfo table = newTable(getIndexMetadata("test8", builder), "test8");
        assertThat(table.clusteredBy()).isEqualTo(new ColumnIdent("id"));

        builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("properties")
            .startObject("content")
            .field("type", "string")
            .field("position", 1)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        table = newTable(getIndexMetadata("test9", builder), "test8");
        assertThat(table.clusteredBy()).isEqualTo(new ColumnIdent("_id"));

        builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("_meta")
            .array("primary_keys", "id", "num")
            .field("routing", "num")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("num")
            .field("type", "long")
            .field("position", 2)
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("position", 3)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        table = newTable(getIndexMetadata("test10", builder), "test10");
        assertThat(table.clusteredBy()).isEqualTo(new ColumnIdent("num"));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        DocTableInfo table = newTable(getIndexMetadata("test11", builder), "test11");
        assertThat(table.clusteredBy()).isEqualTo(new ColumnIdent("_id"));
    }

    @Test
    public void testAutogeneratedPrimaryKey() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        DocTableInfo table = newTable(getIndexMetadata("test11", builder), "test11");
        assertThat(table.primaryKey()).hasSize(1);
        assertThat(table.primaryKey().get(0)).isEqualTo(new ColumnIdent("_id"));
        assertThat(table.hasAutoGeneratedPrimaryKey()).isTrue();
    }

    @Test
    public void testNoAutogeneratedPrimaryKey() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                    .startObject("_meta")
                        .field("primary_keys", "id")
                    .endObject()
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "integer")
                            .field("position", 1)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        DocTableInfo table = newTable(getIndexMetadata("test11", builder), "test11");
        assertThat(table.primaryKey()).hasSize(1);
        assertThat(table.primaryKey().get(0)).isEqualTo(new ColumnIdent("id"));
        assertThat(table.hasAutoGeneratedPrimaryKey()).isFalse();
    }

    @Test
    public void testAnalyzedColumnWithAnalyzer() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                    .startObject("properties")
                        .startObject("content_de")
                            .field("type", "text")
                            .field("position", 1)
                            .field("index", "true")
                            .field("analyzer", "german")
                        .endObject()
                        .startObject("content_en")
                            .field("type", "text")
                            .field("position", 2)
                            .field("analyzer", "english")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        DocTableInfo table = newTable(getIndexMetadata("test_analyzer", builder), "test_analyzer");
        List<Reference> columns = new ArrayList<>(table.columns());
        assertThat(columns).hasSize(2);
        assertThat(columns.get(0).indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(columns.get(0).column().fqn()).isEqualTo("content_de");
        assertThat(columns.get(1).indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(columns.get(1).column().fqn()).isEqualTo("content_en");
    }

    @Test
    public void testGeoPointType() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table foo (p geo_point)");
        assertThat(table.columns()).hasSize(1);
        Reference reference = table.columns().iterator().next();
        assertThat(reference.valueType()).isEqualTo(DataTypes.GEO_POINT);
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingCompat() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table foo (" +
                                                               "id int primary key," +
                                                               "tags array(string)," +
                                                               "o object as (" +
                                                               "   age int," +
                                                               "   name string" +
                                                               ")," +
                                                               "date timestamp with time zone primary key" +
                                                               ") partitioned by (date)");

        assertThat(table.columns()).hasSize(4);
        assertThat(table.primaryKey()).containsExactly(new ColumnIdent("id"), new ColumnIdent("date"));
        assertThat(table.getReference(new ColumnIdent("tags")).valueType()).isEqualTo(
            new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingArrayInsideObject() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement(
            "create table t1 (" +
            "id int primary key," +
            "details object as (names array(string))" +
            ") with (number_of_replicas=0)");
        DataType<?> type = table.getReference(new ColumnIdent("details", "names")).valueType();
        assertThat(type).isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingCompatNoMeta() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table foo (id int, name string)");
        assertThat(table.columns()).hasSize(2);
        assertThat(table.hasAutoGeneratedPrimaryKey()).isTrue();
    }

    private DocTableInfo getDocIndexMetadataFromStatement(String stmt) throws IOException {
        Statement statement = SqlParser.createStatement(stmt);
        Path homeDir = createTempDir();
        FulltextAnalyzerResolver fulltextAnalyzerResolver = new FulltextAnalyzerResolver(
            clusterService,
            new AnalysisRegistry(
                new Environment(Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), homeDir.toString())
                    .build(),
                    homeDir.resolve("config")
                ),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap()
            ));

        CreateTableStatementAnalyzer analyzer = new CreateTableStatementAnalyzer(nodeCtx);

        Analysis analysis = new Analysis(
            new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults()),
            ParamTypeHints.EMPTY,
            Cursors.EMPTY
        );
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
        @SuppressWarnings("unchecked")
        AnalyzedCreateTable analyzedCreateTable = analyzer.analyze(
            (CreateTable<Expression>) statement,
            analysis.paramTypeHints(),
            analysis.transactionContext());
        BoundCreateTable boundCreateTable = analyzedCreateTable.bind(
            new NumberOfShards(clusterService),
            fulltextAnalyzerResolver,
            nodeCtx,
            txnCtx,
            Row.EMPTY,
            SubQueryResults.EMPTY
        );

        Settings.Builder settingsBuilder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", org.elasticsearch.Version.CURRENT)
            .put(boundCreateTable.tableParameter().settings());

        IndexMetadata indexMetadata = IndexMetadata.builder(boundCreateTable.tableName().name())
            .settings(settingsBuilder)
            .putMapping(new MappingMetadata(TestingHelpers.toMapping(boundCreateTable)))
            .build();

        return newTable(indexMetadata, boundCreateTable.tableName().name());
    }

    @Test
    public void testCompoundIndexColumn() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  name string," +
                                                               "  fun string index off," +
                                                               "  INDEX fun_name_ft using fulltext(name, fun)" +
                                                               ")");
        assertThat(table.indexColumns()).hasSize(1);
        assertThat(table.columns()).hasSize(3);
        assertThat(table.indexColumn(ColumnIdent.fromPath("fun_name_ft"))).isExactlyInstanceOf(IndexReference.class);
        IndexReference indexInfo = table.indexColumn(ColumnIdent.fromPath("fun_name_ft"));
        assertThat(indexInfo.indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(indexInfo.column().fqn()).isEqualTo("fun_name_ft");
    }

    @Test
    public void testCompoundIndexColumnNested() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  name string," +
                                                               "  o object as (" +
                                                               "    fun string" +
                                                               "  )," +
                                                               "  INDEX fun_name_ft using fulltext(name, o['fun'])" +
                                                               ")");
        assertThat(table.indexColumns()).hasSize(1);
        assertThat(table.columns()).hasSize(3);
        assertThat(table.indexColumn(ColumnIdent.fromPath("fun_name_ft"))).isExactlyInstanceOf(IndexReference.class);
        IndexReference indexInfo = table.indexColumn(ColumnIdent.fromPath("fun_name_ft"));
        assertThat(indexInfo.indexType()).isEqualTo(IndexType.FULLTEXT);
        assertThat(indexInfo.column().fqn()).isEqualTo("fun_name_ft");
    }

    @Test
    public void testExtractColumnPolicy() throws Exception {
        XContentBuilder ignoredBuilder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", false)
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        DocTableInfo tableIgnored = newTable(getIndexMetadata("test_ignored", ignoredBuilder), "test_ignored");
        assertThat(tableIgnored.columnPolicy()).isEqualTo(ColumnPolicy.IGNORED);

        XContentBuilder strictBuilder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        DocTableInfo tableStrict = newTable(getIndexMetadata("test_strict", strictBuilder), "test_strict");
        assertThat(tableStrict.columnPolicy()).isEqualTo(ColumnPolicy.STRICT);

        XContentBuilder dynamicBuilder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", true)
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        DocTableInfo tableDynamic = newTable(getIndexMetadata("test_dynamic", dynamicBuilder), "test_dynamic");
        assertThat(tableDynamic.columnPolicy()).isEqualTo(ColumnPolicy.DYNAMIC);

        XContentBuilder missingBuilder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        DocTableInfo tableMissing = newTable(getIndexMetadata("test_missing", missingBuilder), "test_missing");
        assertThat(tableMissing.columnPolicy()).isEqualTo(ColumnPolicy.DYNAMIC);

        XContentBuilder wrongBuilder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .field("dynamic", "wrong")
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject()
            .startObject("content")
            .field("type", "string")
            .field("index", "false")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        assertThatThrownBy(() -> newTable(getIndexMetadata("test_wrong", wrongBuilder), "test_wrong"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid column policy: wrong");
    }

    @Test
    public void testCreateArrayMapping() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  tags array(string)," +
                                                               "  scores array(short)" +
                                                               ")");
        assertThat(table.getReference(ColumnIdent.fromPath("tags")).valueType()).isEqualTo(
            new ArrayType<>(DataTypes.STRING));
        assertThat(table.getReference(ColumnIdent.fromPath("scores")).valueType()).isEqualTo(
            new ArrayType<>(DataTypes.SHORT));
    }

    @Test
    public void testCreateObjectArrayMapping() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  tags array(object(strict) as (" +
                                                               "    size double index off," +
                                                               "    numbers array(integer)," +
                                                               "    quote string index using fulltext" +
                                                               "  ))" +
                                                               ")");
        assertThat(
            table.getReference(ColumnIdent.fromPath("tags")).valueType()).isEqualTo(
                new ArrayType<>(
                    ObjectType.builder()
                        .setInnerType("size", DataTypes.DOUBLE)
                        .setInnerType("numbers", DataTypes.INTEGER_ARRAY)
                        .setInnerType("quote", DataTypes.STRING)
                        .build()));
        assertThat(table.getReference(ColumnIdent.fromPath("tags")).columnPolicy()).isEqualTo(
            ColumnPolicy.STRICT);
        assertThat(table.getReference(ColumnIdent.fromPath("tags.size")).valueType()).isEqualTo(
            DataTypes.DOUBLE);
        assertThat(table.getReference(ColumnIdent.fromPath("tags.size")).indexType()).isEqualTo(
            IndexType.NONE);
        assertThat(table.getReference(ColumnIdent.fromPath("tags.numbers")).valueType()).isEqualTo(
            new ArrayType<>(DataTypes.INTEGER));
        assertThat(table.getReference(ColumnIdent.fromPath("tags.quote")).valueType()).isEqualTo(
            DataTypes.STRING);
        assertThat(table.getReference(ColumnIdent.fromPath("tags.quote")).indexType()).isEqualTo(
            IndexType.FULLTEXT);
    }

    @Test
    public void testNoBackwardCompatibleArrayMapping() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("_meta")
            .field("primary_keys", "id")
            .startObject("columns")
            .startObject("array_col")
            .field("collection_type", "array")
            .endObject()
            .startObject("nested")
            .startObject("properties")
            .startObject("inner_nested")
            .field("collection_type", "array")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .startObject("array_col")
            .field("type", "ip")
            .field("position", 3)
            .endObject()
            .startObject("nested")
            .field("position", 4)
            .field("type", "nested")
            .startObject("properties")
            .startObject("inner_nested")
            .field("type", "date")
            .field("position", 5)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata indexMetadata = getIndexMetadata("test1", builder);
        DocTableInfo docIndexMetadata = newTable(indexMetadata, "test1");

        // ARRAY TYPES NOT DETECTED
        assertThat(docIndexMetadata.getReference(ColumnIdent.fromPath("array_col")).valueType()).isEqualTo(
            DataTypes.IP);
        assertThat(docIndexMetadata.getReference(ColumnIdent.fromPath("nested.inner_nested")).valueType()).isEqualTo(
            DataTypes.TIMESTAMPZ);
    }

    @Test
    public void testNewArrayMapping() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("_meta")
            .field("primary_keys", "id")
            .endObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .field("position", 1)
            .endObject()
            .startObject("title")
            .field("type", "string")
            .field("position", 2)
            .field("index", "false")
            .endObject()
            .startObject("array_col")
            .field("type", "array")
            .field("position", 3)
            .startObject("inner")
            .field("type", "ip")
            .field("position", 4)
            .endObject()
            .endObject()
            .startObject("nested")
            .field("position", 5)
            .field("type", "object")
            .startObject("properties")
            .startObject("inner_nested")
            .field("type", "array")
            .startObject("inner")
            .field("type", "date")
            .field("position", 6)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMetadata indexMetadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(indexMetadata, "test1");
        assertThat(table.getReference(ColumnIdent.fromPath("array_col")).valueType()).isEqualTo(
            new ArrayType<>(DataTypes.IP));
        assertThat(table.getReference(ColumnIdent.fromPath("nested.inner_nested")).valueType()).isEqualTo(
            new ArrayType<>(DataTypes.TIMESTAMPZ));
    }

    @Test
    public void testStringArrayWithFulltextIndex() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement(
            "create table t (tags array(string) index using fulltext)");

        Reference reference = table.columns().iterator().next();
        assertThat(reference.valueType()).isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testCreateTableWithNestedPrimaryKey() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (o object as (x int primary key))");
        assertThat(table.primaryKey()).containsExactly(new ColumnIdent("o", "x"));

        table = getDocIndexMetadataFromStatement("create table t (x object as (y object as (z int primary key)))");
        assertThat(table.primaryKey()).containsExactly(new ColumnIdent("x", Arrays.asList("y", "z")));
    }

    @Test
    public void testSchemaWithGeneratedColumn() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject("_meta")
                    .startObject("generated_columns")
                        .field("week", "date_trunc('week', ts)")
                    .endObject()
                .endObject()
                .startObject("properties")
                    .startObject("ts")
                    .field("type", "date")
                    .field("position", 1)
                    .endObject()
                    .startObject("week")
                    .field("type", "long")
                    .field("position", 2)
                    .endObject()
                .endObject()
            .endObject();

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        assertThat(table.columns()).hasSize(2);
        Reference week = table.getReference(new ColumnIdent("week"));
        assertThat(week)
            .isNotNull()
            .isExactlyInstanceOf(GeneratedReference.class);
        assertThat(((GeneratedReference) week).formattedGeneratedExpression()).isEqualTo("date_trunc('week', ts)");
        assertThat(((GeneratedReference) week).generatedExpression())
            .isFunction("_cast",
                arg1 -> assertThat(arg1).isFunction("date_trunc", isLiteral("week"), isReference("ts")),
                arg2 -> assertThat(arg2).isLiteral("bigint")
            );
        assertThat(((GeneratedReference) week).referencedReferences()).satisfiesExactly(isReference("ts"));
    }

    @Test
    public void testArrayAsGeneratedColumn() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t1 (x as ([10, 20]))");
        GeneratedReference generatedReference = table.generatedColumns().get(0);
        assertThat(generatedReference.valueType()).isEqualTo(new ArrayType<>(DataTypes.INTEGER));
    }

    @Test
    public void testColumnWithDefaultExpression() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t1 (" +
                                                               " ts timestamp with time zone default current_timestamp)");
        Reference reference = table.getReference(new ColumnIdent("ts"));
        assertThat(reference.valueType()).isEqualTo(DataTypes.TIMESTAMPZ);
        assertThat(reference.defaultExpression()).isFunction("current_timestamp");
    }

    @Test
    public void testTimestampColumnReferences() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                    .startObject("properties")
                        .startObject("tz")
                            .field("type", "date")
                            .field("position", 1)
                        .endObject()
                        .startObject("t")
                            .field("type", "date")
                            .field("position", 2)
                            .field("ignore_timezone", true)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        DocTableInfo table = newTable(getIndexMetadata("test", builder), "test");

        assertThat(table.columns()).hasSize(2);
        assertThat(table.getReference(new ColumnIdent("tz")).valueType()).isEqualTo(DataTypes.TIMESTAMPZ);
        assertThat(table.getReference(new ColumnIdent("t")).valueType()).isEqualTo(DataTypes.TIMESTAMP);
    }

    @Test
    public void testColumnStoreBooleanIsParsedCorrectly() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement(
                "create table t1 (x string STORAGE WITH (columnstore = false))");
        assertThat(table.columns().iterator().next().hasDocValues()).isFalse();
    }

    @Test
    public void test_resolve_inner_object_types() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                    .startObject("object")
                    .field("position", 1)
                        .field("type", "object")
                            .startObject("properties")
                                .startObject("nestedObject")
                                .field("position", 2)
                                    .field("type", "object")
                                    .startObject("properties")
                                        .startObject("nestedNestedString")
                                            .field("type", "string")
                                            .field("position", 3)
                                        .endObject()
                                    .endObject()
                                .endObject()
                                .startObject("nestedString")
                                    .field("type", "string")
                                    .field("position", 4)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test", builder);
        DocTableInfo table = newTable(metadata, "test");

        ObjectType objectType = (ObjectType) table.getReference(new ColumnIdent("object")).valueType();
        assertThat(objectType.resolveInnerType(List.of("nestedString"))).isEqualTo(DataTypes.STRING);
        assertThat(objectType.resolveInnerType(List.of("nestedObject")).id()).isEqualTo(ObjectType.ID);
        assertThat(objectType.resolveInnerType(List.of("nestedObject", "nestedNestedString"))).isEqualTo(DataTypes.STRING);
    }

    @Test
    public void test_nested_geo_shape_column_is_not_added_as_top_level_column() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement(
            "create table tbl (x int, y object as (z geo_shape))");
        assertThat(table.columns()).satisfiesExactlyInAnyOrder(isReference("x"), isReference("y"));
        assertThat(table.getReference(new ColumnIdent("y", "z"))).isNotNull();
    }

    @Test
    public void test_resolve_string_type_with_length_from_mappings_with_text_and_keyword_types() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                    .startObject("properties")
                        .startObject("col")
                            .field("type", "keyword")
                            .field("position", 1)
                            .field("length_limit", 10)
                            .field("index", "false")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        var docIndexMetadata = newTable(getIndexMetadata("test", builder), "test");

        var column = docIndexMetadata.getReference(new ColumnIdent("col"));
        assertThat(column).isNotNull();
        assertThat(column.valueType()).isEqualTo(StringType.of(10));
    }

    @Test
    public void test_geo_shape_column_has_generated_expression() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (g geo_shape generated always as 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
        Reference reference = table.getReference(new ColumnIdent("g"));
        assertThat(reference.valueType()).isEqualTo(DataTypes.GEO_SHAPE);
        GeneratedReference genRef = (GeneratedReference) reference;
        assertThat(genRef.formattedGeneratedExpression()).isEqualTo("'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))'");
    }

    @Test
    public void test_geo_shape_column_has_default_expression() throws Exception {
        DocTableInfo table = getDocIndexMetadataFromStatement("create table t (g geo_shape default 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))')");
        Reference reference = table.getReference(new ColumnIdent("g"));
        assertThat(reference.valueType()).isEqualTo(DataTypes.GEO_SHAPE);
        assertThat(reference.defaultExpression().toString(Style.UNQUALIFIED))
            .isEqualTo("'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))'");
    }

    @Test
    public void test_column_of_index_has_nullable_from_real_column() throws Exception {
        var table = getDocIndexMetadataFromStatement(
            "create table tbl (x string, index ft using fulltext (x))");
        var indexReference = table.indexColumn(new ColumnIdent("ft"));
        assertThat(indexReference.columns().get(0).isNullable()).isTrue();

        table = getDocIndexMetadataFromStatement(
            "create table tbl (x string not null, index ft using fulltext (x))");
        indexReference = table.indexColumn(new ColumnIdent("ft"));
        assertThat(indexReference.columns().get(0).isNullable()).isFalse();
    }

    @Test
    public void test_copy_to_ft_refers_to_sources() throws Exception {
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("properties")
            .startObject("description")
            .field("type", "string")
            .field("position", 1)
            .array("copy_to", "description_ft")
            .endObject()
            .startObject("description_ft")
            .field("type", "string")
            .field("position", 2)
            .endObject()
            .endObject()
            .endObject();

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocTableInfo table = newTable(metadata, "test1");

        assertThat(table.indexColumns()).hasSize(1);
        IndexReference ref = table.indexColumns().iterator().next();
        assertThat(ref.columns()).satisfiesExactly(
            x -> assertThat(x).isReference().hasName("description")
        );
    }

    @Test
    public void test_generated_columns_expression_has_specified_type() throws Exception {
        var table = getDocIndexMetadataFromStatement("""
            create table tbl (
                x int,
                y int,
                sum long generated always as x + y
            )
            """
        );
        assertThat(table.generatedColumns()).satisfiesExactly(
            sum -> {
                assertThat(sum.generatedExpression().valueType()).isEqualTo(DataTypes.LONG);
                assertThat(sum.valueType()).isEqualTo(DataTypes.LONG);
            }
        );
    }
}
