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

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.Constants;
import io.crate.analyze.Analysis;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.CreateTableStatementAnalyzer;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.ParamTypeHints;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.expression.udf.UserDefinedFunctionService;
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
import io.crate.metadata.view.ViewInfoFactory;
import io.crate.planner.node.ddl.CreateTablePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Statement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

// @formatter:off
public class DocIndexMetadataTest extends CrateDummyClusterServiceUnitTest {

    private UserDefinedFunctionService udfService;
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
            .putMapping(new MappingMetadata(Constants.DEFAULT_MAPPING_TYPE, mappingSource));
        return mdBuilder.build();
    }

    private DocIndexMetadata newMeta(IndexMetadata metadata, String name) throws IOException {
        return new DocIndexMetadata(nodeCtx, metadata, new RelationName(Schemas.DOC_SCHEMA_NAME, name), null).build();
    }

    @Before
    public void setupUdfService() {
        nodeCtx = createNodeContext();
        udfService = new UserDefinedFunctionService(clusterService, new DocTableInfoFactory(nodeCtx), nodeCtx);
    }

    @Test
    public void testNestedColumnIdent() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        Reference reference = md.references().get(new ColumnIdent("person", Arrays.asList("addresses", "city")));
        assertNotNull(reference);
    }

    @Test
    public void testExtractObjectColumnDefinitions() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");
        assertThat(md.columns().size(), is(4));
        assertThat(md.references().size(), is(20));
        assertThat(md.references().get(new ColumnIdent("implicit_dynamic")).columnPolicy(), is(ColumnPolicy.DYNAMIC));
        assertThat(md.references().get(new ColumnIdent("explicit_dynamic")).columnPolicy(), is(ColumnPolicy.DYNAMIC));
        assertThat(md.references().get(new ColumnIdent("ignored")).columnPolicy(), is(ColumnPolicy.IGNORED));
        assertThat(md.references().get(new ColumnIdent("strict")).columnPolicy(), is(ColumnPolicy.STRICT));
    }

    @Test
    public void testExtractColumnDefinitions() throws Exception {
        // @formatter:off
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        assertThat(md.columns().size(), is(11));
        assertThat(md.references().size(), is(23));

        Reference birthday = md.references().get(new ColumnIdent("person", "birthday"));
        assertThat(birthday.valueType(), is(DataTypes.TIMESTAMPZ));
        assertThat(birthday.indexType(), is(IndexType.PLAIN));
        assertThat(birthday.defaultExpression(), is(nullValue()));

        Reference integerIndexed = md.references().get(new ColumnIdent("integerIndexed"));
        assertThat(integerIndexed.indexType(), is(IndexType.PLAIN));
        assertThat(integerIndexed.defaultExpression(), is(nullValue()));

        Reference integerIndexedBWC = md.references().get(new ColumnIdent("integerIndexedBWC"));
        assertThat(integerIndexedBWC.indexType(), is(IndexType.PLAIN));
        assertThat(integerIndexedBWC.defaultExpression(), is(nullValue()));

        Reference integerNotIndexed = md.references().get(new ColumnIdent("integerNotIndexed"));
        assertThat(integerNotIndexed.indexType(), is(IndexType.NONE));
        assertThat(integerNotIndexed.defaultExpression(), is(nullValue()));

        Reference integerNotIndexedBWC = md.references().get(new ColumnIdent("integerNotIndexedBWC"));
        assertThat(integerNotIndexedBWC.indexType(), is(IndexType.NONE));
        assertThat(integerNotIndexedBWC.defaultExpression(), is(nullValue()));

        Reference stringNotIndexed = md.references().get(new ColumnIdent("stringNotIndexed"));
        assertThat(stringNotIndexed.indexType(), is(IndexType.NONE));
        assertThat(stringNotIndexed.defaultExpression(), is(nullValue()));

        Reference stringNotIndexedBWC = md.references().get(new ColumnIdent("stringNotIndexedBWC"));
        assertThat(stringNotIndexedBWC.indexType(), is(IndexType.NONE));
        assertThat(stringNotIndexedBWC.defaultExpression(), is(nullValue()));

        Reference stringNotAnalyzed = md.references().get(new ColumnIdent("stringNotAnalyzed"));
        assertThat(stringNotAnalyzed.indexType(), is(IndexType.PLAIN));
        assertThat(stringNotAnalyzed.defaultExpression(), is(nullValue()));

        Reference stringNotAnalyzedBWC = md.references().get(new ColumnIdent("stringNotAnalyzedBWC"));
        assertThat(stringNotAnalyzedBWC.indexType(), is(IndexType.PLAIN));
        assertThat(stringNotAnalyzedBWC.defaultExpression(), is(nullValue()));

        Reference stringAnalyzed = md.references().get(new ColumnIdent("stringAnalyzed"));
        assertThat(stringAnalyzed.indexType(), is(IndexType.FULLTEXT));
        assertThat(stringAnalyzed.defaultExpression(), is(nullValue()));

        Reference stringAnalyzedBWC = md.references().get(new ColumnIdent("stringAnalyzedBWC"));
        assertThat(stringAnalyzedBWC.indexType(), is(IndexType.FULLTEXT));
        assertThat(stringAnalyzedBWC.defaultExpression(), is(nullValue()));

        assertThat(
            Lists2.map(md.references().values(), r -> r.column().fqn()),
            containsInAnyOrder("_doc", "_fetchid", "_id", "_raw", "_score", "_uid", "_version", "_docid", "_seq_no",
                "_primary_term", "integerIndexed", "integerIndexedBWC", "integerNotIndexed", "integerNotIndexedBWC",
                "person", "person.birthday", "person.first_name",
                "stringAnalyzed", "stringAnalyzedBWC", "stringNotAnalyzed", "stringNotAnalyzedBWC",
                "stringNotIndexed", "stringNotIndexedBWC"));
    }

    @Test
    public void testExtractColumnDefinitionsWithDefaultExpression() throws Exception {
        // @formatter:off
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
                .endObject()
            .endObject();
        // @formatter:on

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocIndexMetadata md = newMeta(metadata, "test1");

        assertThat(md.columns().size(), is(6));
        assertThat(md.references().size(), is(16));

        Reference birthday = md.references().get(new ColumnIdent("birthday"));
        assertThat(birthday.valueType(), is(DataTypes.TIMESTAMPZ));
        assertThat(birthday.defaultExpression(), isFunction("current_timestamp", List.of(DataTypes.INTEGER)));

        Reference integerIndexed = md.references().get(new ColumnIdent("integerIndexed"));
        assertThat(integerIndexed.indexType(), is(IndexType.PLAIN));
        assertThat(integerIndexed.defaultExpression(), isLiteral(1));


        Reference integerNotIndexed = md.references().get(new ColumnIdent("integerNotIndexed"));
        assertThat(integerNotIndexed.indexType(), is(IndexType.NONE));
        assertThat(integerNotIndexed.defaultExpression(), isLiteral(1));

        Reference stringNotIndexed = md.references().get(new ColumnIdent("stringNotIndexed"));
        assertThat(stringNotIndexed.indexType(), is(IndexType.NONE));
        assertThat(stringNotIndexed.defaultExpression(), isLiteral("default"));

        Reference stringNotAnalyzed = md.references().get(new ColumnIdent("stringNotAnalyzed"));
        assertThat(stringNotAnalyzed.indexType(), is(IndexType.PLAIN));
        assertThat(stringNotAnalyzed.defaultExpression(), isLiteral("default"));

        Reference stringAnalyzed = md.references().get(new ColumnIdent("stringAnalyzed"));
        assertThat(stringAnalyzed.indexType(), is(IndexType.FULLTEXT));
        assertThat(stringAnalyzed.defaultExpression(), isLiteral("default"));

        assertThat(
            Lists2.map(md.references().values(), r -> r.column().fqn()),
            containsInAnyOrder(
                "_raw", "_doc", "_seq_no", "_version", "_id", "_uid",
                "_score", "_fetchid", "_primary_term", "_docid",
                "birthday", "integerIndexed", "integerNotIndexed",
                "stringAnalyzed", "stringNotAnalyzed", "stringNotIndexed"));
    }

    @Test
    public void testExtractPartitionedByColumns() throws Exception {
        // @formatter:off
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        assertEquals(6, md.columns().size());
        assertEquals(19, md.references().size());
        assertEquals(1, md.partitionedByColumns().size());
        assertEquals(DataTypes.TIMESTAMPZ, md.partitionedByColumns().get(0).valueType());
        assertThat(md.partitionedByColumns().get(0).column().fqn(), is("datum"));

        assertThat(md.partitionedBy().size(), is(1));
        assertThat(md.partitionedBy().get(0), is(ColumnIdent.fromPath("datum")));
    }

    @Test
    public void testExtractPartitionedByWithPartitionedByInColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        // partitioned by column is not added twice
        assertEquals(2, md.columns().size());
        assertEquals(12, md.references().size());
        assertEquals(1, md.partitionedByColumns().size());
    }

    @Test
    public void testExtractPartitionedByWithNestedPartitionedByInColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        // partitioned by column is not added twice
        assertEquals(2, md.columns().size());
        assertEquals(13, md.references().size());
        assertEquals(1, md.partitionedByColumns().size());
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
                map.put(entry.getKey(), sortProperties((Map) entry.getValue(), sortNext));
            } else {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        return map;
    }

    @Test
    public void testExtractColumnDefinitionsFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        IndexMetadata metadata = getIndexMetadata("test2", builder);
        DocIndexMetadata md = newMeta(metadata, "test2");
        assertThat(md.columns(), hasSize(0));
    }

    @Test
    public void testDocSysColumnReferences() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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

        DocIndexMetadata metadata = newMeta(getIndexMetadata("test", builder), "test");
        Map<ColumnIdent, Reference> references = metadata.references();
        Reference id = references.get(new ColumnIdent("_id"));
        assertNotNull(id);

        Reference version = references.get(new ColumnIdent("_version"));
        assertNotNull(version);

        Reference score = references.get(new ColumnIdent("_score"));
        assertNotNull(score);

        Reference docId = references.get(new ColumnIdent("_docid"));
        assertNotNull(docId);
    }

    @Test
    public void testExtractPrimaryKey() throws Exception {

        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test3");


        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey(), contains(new ColumnIdent("id")));

        builder = XContentFactory.jsonBuilder()
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

        md = newMeta(getIndexMetadata("test4", builder), "test4");
        assertThat(md.primaryKey().size(), is(1)); // _id is always the fallback primary key

        builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        md = newMeta(getIndexMetadata("test5", builder), "test5");
        assertThat(md.primaryKey().size(), is(1));
    }

    @Test
    public void testExtractMultiplePrimaryKeys() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test_multi_pk");
        assertThat(md.primaryKey().size(), is(2));
        assertThat(md.primaryKey(), hasItems(ColumnIdent.fromPath("id"), ColumnIdent.fromPath("title")));
    }

    @Test
    public void testExtractCheckConstraints() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test3");
        assertThat(md.checkConstraints().size(), is(2));
        assertThat(md.checkConstraints()
                       .stream()
                       .map(CheckConstraint::expressionStr)
                       .collect(Collectors.toList()),
                   containsInAnyOrder(
                       equalTo("id >= 0"),
                       equalTo("title != 'Programming Clojure'")
        ));
    }

    @Test
    public void testExtractNoPrimaryKey() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test_no_pk");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey(), hasItems(ColumnIdent.fromPath("_id")));

        builder = XContentFactory.jsonBuilder()
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
        md = newMeta(metadata, "test_no_pk2");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey(), hasItems(ColumnIdent.fromPath("_id")));
    }

    @Test
    public void testSchemaWithNotNullColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test_notnull_columns");

        assertThat(
            md.columns().stream().map(Reference::isNullable).collect(Collectors.toList()),
            contains(false, false)
        );
    }

    @Test
    public void testSchemaWithNotNullNestedColumns() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test_notnull_columns");

        ColumnIdent level1 = new ColumnIdent("nested", "level1");
        ColumnIdent level2 = new ColumnIdent("nested", Arrays.asList("level1", "level2"));
        assertThat(md.notNullColumns(), containsInAnyOrder(level1, level2));
        assertThat(md.references().get(level1).isNullable(), is(false));
        assertThat(md.references().get(level2).isNullable(), is(false));
    }

    @Test
    public void testSchemaWithNotNullGeneratedColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        assertThat(md.columns().size(), is(2));
        Reference week = md.references().get(new ColumnIdent("week"));
        assertThat(week, Matchers.notNullValue());
        assertThat(week.isNullable(), is(false));
        assertThat(week, instanceOf(GeneratedReference.class));
        assertThat(((GeneratedReference) week).formattedGeneratedExpression(), is("date_trunc('week', ts)"));
        assertThat(((GeneratedReference) week).generatedExpression(), isFunction("date_trunc", isLiteral("week"), isReference("ts")));
        assertThat(((GeneratedReference) week).referencedReferences(), contains(isReference("ts")));
    }

    @Test
    public void extractRoutingColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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

        DocIndexMetadata md = newMeta(getIndexMetadata("test8", builder), "test8");
        assertThat(md.routingCol(), is(new ColumnIdent("id")));

        builder = XContentFactory.jsonBuilder()
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

        md = newMeta(getIndexMetadata("test9", builder), "test8");
        assertThat(md.routingCol(), is(new ColumnIdent("_id")));

        builder = XContentFactory.jsonBuilder()
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

        md = newMeta(getIndexMetadata("test10", builder), "test10");
        assertThat(md.routingCol(), is(new ColumnIdent("num")));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        DocIndexMetadata md = newMeta(getIndexMetadata("test11", builder), "test11");
        assertThat(md.routingCol(), is(new ColumnIdent("_id")));
    }

    @Test
    public void testAutogeneratedPrimaryKey() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(Constants.DEFAULT_MAPPING_TYPE)
            .endObject()
            .endObject();
        DocIndexMetadata md = newMeta(getIndexMetadata("test11", builder), "test11");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey().get(0), is(new ColumnIdent("_id")));
        assertThat(md.hasAutoGeneratedPrimaryKey(), is(true));
    }

    @Test
    public void testNoAutogeneratedPrimaryKey() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(getIndexMetadata("test11", builder), "test11");
        assertThat(md.primaryKey().size(), is(1));
        assertThat(md.primaryKey().get(0), is(new ColumnIdent("id")));
        assertThat(md.hasAutoGeneratedPrimaryKey(), is(false));
    }

    @Test
    public void testAnalyzedColumnWithAnalyzer() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(getIndexMetadata("test_analyzer", builder), "test_analyzer");
        List<Reference> columns = new ArrayList<>(md.columns());
        assertThat(columns.size(), is(2));
        assertThat(columns.get(0).indexType(), is(IndexType.FULLTEXT));
        assertThat(columns.get(0).column().fqn(), is("content_de"));
        assertThat(columns.get(1).indexType(), is(IndexType.FULLTEXT));
        assertThat(columns.get(1).column().fqn(), is("content_en"));
    }

    @Test
    public void testGeoPointType() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table foo (p geo_point)");
        assertThat(md.columns().size(), is(1));
        Reference reference = md.columns().iterator().next();
        assertThat(reference.valueType(), equalTo(DataTypes.GEO_POINT));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingCompat() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table foo (" +
                                                               "id int primary key," +
                                                               "tags array(string)," +
                                                               "o object as (" +
                                                               "   age int," +
                                                               "   name string" +
                                                               ")," +
                                                               "date timestamp with time zone primary key" +
                                                               ") partitioned by (date)");

        assertThat(md.columns().size(), is(4));
        assertThat(md.primaryKey(), Matchers.contains(new ColumnIdent("id"), new ColumnIdent("date")));
        assertThat(md.references().get(new ColumnIdent("tags")).valueType(), is(new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingArrayInsideObject() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement(
            "create table t1 (" +
            "id int primary key," +
            "details object as (names array(string))" +
            ") with (number_of_replicas=0)");
        DataType type = md.references().get(new ColumnIdent("details", "names")).valueType();
        assertThat(type, Matchers.equalTo(new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCreateTableMappingGenerationAndParsingCompatNoMeta() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table foo (id int, name string)");
        assertThat(md.columns().size(), is(2));
        assertThat(md.hasAutoGeneratedPrimaryKey(), is(true));
    }

    private DocIndexMetadata getDocIndexMetadataFromStatement(String stmt) throws IOException {
        Statement statement = SqlParser.createStatement(stmt);

        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeCtx);
        ViewInfoFactory viewInfoFactory = new ViewInfoFactory(() -> null);
        DocSchemaInfo docSchemaInfo = new DocSchemaInfo(Schemas.DOC_SCHEMA_NAME, clusterService, nodeCtx, udfService, viewInfoFactory, docTableInfoFactory);
        Path homeDir = createTempDir();
        Schemas schemas = new Schemas(
                Map.of("doc", docSchemaInfo),
                clusterService,
                new DocSchemaInfoFactory(docTableInfoFactory, viewInfoFactory, nodeCtx, udfService));
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

        Analysis analysis = new Analysis(new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults()), ParamTypeHints.EMPTY);
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
        AnalyzedCreateTable analyzedCreateTable = analyzer.analyze(
            (CreateTable<Expression>) statement,
            analysis.paramTypeHints(),
            analysis.transactionContext());
        BoundCreateTable analyzedStatement = CreateTablePlan.bind(
            analyzedCreateTable,
            txnCtx,
            nodeCtx,
            Row.EMPTY,
            SubQueryResults.EMPTY,
            new NumberOfShards(clusterService),
            schemas,
            fulltextAnalyzerResolver
        );

        Settings.Builder settingsBuilder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", org.elasticsearch.Version.CURRENT)
            .put(analyzedStatement.tableParameter().settings());

        IndexMetadata indexMetadata = IndexMetadata.builder(analyzedStatement.tableIdent().name())
            .settings(settingsBuilder)
            .putMapping(new MappingMetadata(Constants.DEFAULT_MAPPING_TYPE, analyzedStatement.mapping()))
            .build();

        return newMeta(indexMetadata, analyzedStatement.tableIdent().name());
    }

    @Test
    public void testCompoundIndexColumn() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  name string," +
                                                               "  fun string index off," +
                                                               "  INDEX fun_name_ft using fulltext(name, fun)" +
                                                               ")");
        assertThat(md.indices().size(), is(1));
        assertThat(md.columns().size(), is(3));
        assertThat(md.indices().get(ColumnIdent.fromPath("fun_name_ft")), instanceOf(IndexReference.class));
        IndexReference indexInfo = md.indices().get(ColumnIdent.fromPath("fun_name_ft"));
        assertThat(indexInfo.indexType(), is(IndexType.FULLTEXT));
        assertThat(indexInfo.column().fqn(), is("fun_name_ft"));
    }

    @Test
    public void testCompoundIndexColumnNested() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  name string," +
                                                               "  o object as (" +
                                                               "    fun string" +
                                                               "  )," +
                                                               "  INDEX fun_name_ft using fulltext(name, o['fun'])" +
                                                               ")");
        assertThat(md.indices().size(), is(1));
        assertThat(md.columns().size(), is(3));
        assertThat(md.indices().get(ColumnIdent.fromPath("fun_name_ft")), instanceOf(IndexReference.class));
        IndexReference indexInfo = md.indices().get(ColumnIdent.fromPath("fun_name_ft"));
        assertThat(indexInfo.indexType(), is(IndexType.FULLTEXT));
        assertThat(indexInfo.column().fqn(), is("fun_name_ft"));
    }

    @Test
    public void testExtractColumnPolicy() throws Exception {
        XContentBuilder ignoredBuilder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata mdIgnored = newMeta(getIndexMetadata("test_ignored", ignoredBuilder), "test_ignored");
        assertThat(mdIgnored.columnPolicy(), is(ColumnPolicy.IGNORED));

        XContentBuilder strictBuilder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata mdStrict = newMeta(getIndexMetadata("test_strict", strictBuilder), "test_strict");
        assertThat(mdStrict.columnPolicy(), is(ColumnPolicy.STRICT));

        XContentBuilder dynamicBuilder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata mdDynamic = newMeta(getIndexMetadata("test_dynamic", dynamicBuilder), "test_dynamic");
        assertThat(mdDynamic.columnPolicy(), is(ColumnPolicy.DYNAMIC));

        XContentBuilder missingBuilder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata mdMissing = newMeta(getIndexMetadata("test_missing", missingBuilder), "test_missing");
        assertThat(mdMissing.columnPolicy(), is(ColumnPolicy.DYNAMIC));

        XContentBuilder wrongBuilder = XContentFactory.jsonBuilder()
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
        expectedException.expectMessage("Invalid column policy: wrong");
        newMeta(getIndexMetadata("test_wrong", wrongBuilder), "test_wrong");
    }

    @Test
    public void testCreateArrayMapping() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  tags array(string)," +
                                                               "  scores array(short)" +
                                                               ")");
        assertThat(md.references().get(ColumnIdent.fromPath("tags")).valueType(),
            is(new ArrayType(DataTypes.STRING)));
        assertThat(md.references().get(ColumnIdent.fromPath("scores")).valueType(),
            is(new ArrayType(DataTypes.SHORT)));
    }

    @Test
    public void testCreateObjectArrayMapping() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table t (" +
                                                               "  id integer primary key," +
                                                               "  tags array(object(strict) as (" +
                                                               "    size double index off," +
                                                               "    numbers array(integer)," +
                                                               "    quote string index using fulltext" +
                                                               "  ))" +
                                                               ")");
        assertThat(
            md.references().get(ColumnIdent.fromPath("tags")).valueType(),
            is(new ArrayType<>(
                ObjectType.builder()
                    .setInnerType("size", DataTypes.DOUBLE)
                    .setInnerType("numbers", DataTypes.INTEGER_ARRAY)
                    .setInnerType("quote", DataTypes.STRING)
                    .build()))
        );
        assertThat(md.references().get(ColumnIdent.fromPath("tags")).columnPolicy(),
            is(ColumnPolicy.STRICT));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.size")).valueType(),
            is(DataTypes.DOUBLE));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.size")).indexType(),
            is(IndexType.NONE));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.numbers")).valueType(),
            is(new ArrayType<>(DataTypes.INTEGER)));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.quote")).valueType(),
            is(DataTypes.STRING));
        assertThat(md.references().get(ColumnIdent.fromPath("tags.quote")).indexType(),
            is(IndexType.FULLTEXT));
    }

    @Test
    public void testNoBackwardCompatibleArrayMapping() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata docIndexMetadata = newMeta(indexMetadata, "test1");

        // ARRAY TYPES NOT DETECTED
        assertThat(docIndexMetadata.references().get(ColumnIdent.fromPath("array_col")).valueType(),
            is(DataTypes.IP));
        assertThat(docIndexMetadata.references().get(ColumnIdent.fromPath("nested.inner_nested")).valueType(),
            is(DataTypes.TIMESTAMPZ));
    }

    @Test
    public void testNewArrayMapping() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata docIndexMetadata = newMeta(indexMetadata, "test1");
        assertThat(docIndexMetadata.references().get(ColumnIdent.fromPath("array_col")).valueType(),
            is(new ArrayType(DataTypes.IP)));
        assertThat(docIndexMetadata.references().get(ColumnIdent.fromPath("nested.inner_nested")).valueType(),
            is(new ArrayType(DataTypes.TIMESTAMPZ)));
    }

    @Test
    public void testStringArrayWithFulltextIndex() throws Exception {
        DocIndexMetadata metadata = getDocIndexMetadataFromStatement(
            "create table t (tags array(string) index using fulltext)");

        Reference reference = metadata.columns().iterator().next();
        assertThat(reference.valueType(), equalTo(new ArrayType(DataTypes.STRING)));
    }

    @Test
    public void testCreateTableWithNestedPrimaryKey() throws Exception {
        DocIndexMetadata metadata = getDocIndexMetadataFromStatement("create table t (o object as (x int primary key))");
        assertThat(metadata.primaryKey(), contains(new ColumnIdent("o", "x")));

        metadata = getDocIndexMetadataFromStatement("create table t (x object as (y object as (z int primary key)))");
        assertThat(metadata.primaryKey(), contains(new ColumnIdent("x", Arrays.asList("y", "z"))));
    }

    @Test
    public void testSchemaWithGeneratedColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test1");

        assertThat(md.columns().size(), is(2));
        Reference week = md.references().get(new ColumnIdent("week"));
        assertThat(week, Matchers.notNullValue());
        assertThat(week, instanceOf(GeneratedReference.class));
        assertThat(((GeneratedReference) week).formattedGeneratedExpression(), is("date_trunc('week', ts)"));
        assertThat(((GeneratedReference) week).generatedExpression(), isFunction("date_trunc", isLiteral("week"), isReference("ts")));
        assertThat(((GeneratedReference) week).referencedReferences(), contains(isReference("ts")));
    }

    @Test
    public void testCopyToWithoutMetaIndices() throws Exception {
        // regression test... this mapping used to cause an NPE
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
            .field("analyzer", "english")
            .endObject()
            .endObject()
            .endObject();

        IndexMetadata metadata = getIndexMetadata("test1", builder);
        DocIndexMetadata md = newMeta(metadata, "test1");

        assertThat(md.indices().size(), is(1));
        assertThat(md.indices().keySet().iterator().next(), is(new ColumnIdent("description_ft")));
    }

    @Test
    public void testArrayAsGeneratedColumn() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table t1 (x as ([10, 20]))");
        GeneratedReference generatedReference = md.generatedColumnReferences().get(0);
        assertThat(generatedReference.valueType(), is(new ArrayType<>(DataTypes.INTEGER)));
    }

    @Test
    public void testColumnWithDefaultExpression() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement("create table t1 (" +
                                                               " ts timestamp with time zone default current_timestamp)");
        Reference reference = md.references().get(new ColumnIdent("ts"));
        assertThat(reference.valueType(), is(DataTypes.TIMESTAMPZ));
        assertThat(reference.defaultExpression(), isFunction("current_timestamp"));
    }

    @Test
    public void testTimestampColumnReferences() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(getIndexMetadata("test", builder), "test");

        assertThat(md.columns().size(), is(2));
        assertThat(md.references().get(new ColumnIdent("tz")).valueType(), is(DataTypes.TIMESTAMPZ));
        assertThat(md.references().get(new ColumnIdent("t")).valueType(), is(DataTypes.TIMESTAMP));
    }

    @Test
    public void testColumnStoreBooleanIsParsedCorrectly() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement(
            "create table t1 (x string STORAGE WITH (columnstore = false))");
        assertThat(md.columns().iterator().next().hasDocValues(), is(false));
    }

    @Test
    public void test_resolve_inner_object_types() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        DocIndexMetadata md = newMeta(metadata, "test");

        ObjectType objectType = (ObjectType) md.references().get(new ColumnIdent("object")).valueType();
        assertThat(objectType.resolveInnerType(List.of("nestedString")), is(DataTypes.STRING));
        assertThat(objectType.resolveInnerType(List.of("nestedObject")).id(), is(ObjectType.ID));
        assertThat(objectType.resolveInnerType(List.of("nestedObject", "nestedNestedString")), is(DataTypes.STRING));
    }

    @Test
    public void test_nested_geo_shape_column_is_not_added_as_top_level_column() throws Exception {
        DocIndexMetadata md = getDocIndexMetadataFromStatement(
            "create table tbl (x int, y object as (z geo_shape))");
        assertThat(md.columns(), contains(isReference("x"), isReference("y")));
        assertThat(md.references(), hasKey(new ColumnIdent("y", "z")));
    }

    @Test
    public void test_resolve_string_type_with_length_from_mappings_with_text_and_keyword_types() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
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
        var docIndexMetadata = newMeta(getIndexMetadata("test", builder), "test");

        var column = docIndexMetadata.references().get(new ColumnIdent("col"));
        assertThat(column, is(not(nullValue())));
        assertThat(column.valueType(), is(StringType.of(10)));
    }
}
