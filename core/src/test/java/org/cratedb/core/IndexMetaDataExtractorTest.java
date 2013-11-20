package org.cratedb.core;

import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class IndexMetaDataExtractorTest extends AbstractCrateNodesTests {

    private static InternalNode node = null;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Before
    public void before() {
        if (node == null) {
            node = (InternalNode)startNode(getClass().getName());
        }
    }

    @AfterClass
    public static void closeNode() {
        if (node != null) {
            node.stop();
            node.close();
        }
        node = null;
    }

    private void refresh() {
        refresh(node.client());
    }
    private IndexMetaData getIndexMetaData(String indexName) {
        ClusterStateResponse stateResponse = node.client().admin().cluster()
                .prepareState().execute().actionGet();
        return stateResponse.getState().metaData().indices().get(indexName);
    }

    @Test
    public void testExtractColumnDefinitions() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("title")
                                .field("type", "string")
                                .field("index", "no")
                            .endObject()
                            .startObject("datum")
                                .field("type", "date")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "analyzed")
                                .field("analyzer", "standard")
                            .endObject()
                            .startObject("person")
                                .startObject("properties")
                                    .startObject("first_name")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("birthday")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("nested")
                                .field("type", "nested")
                                .startObject("properties")
                                    .startObject("inner_nested")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test1")
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();
        IndexMetaData metaData = getIndexMetaData("test1");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();

        assertEquals(9, columnDefinitions.size());

        assertThat(columnDefinitions.get(0).columnName, is("content"));
        assertThat(columnDefinitions.get(0).dataType, is("string"));
        assertThat(columnDefinitions.get(0).ordinalPosition, is(1));
        assertThat(columnDefinitions.get(0).tableName, is("test1"));
        assertFalse(columnDefinitions.get(0).dynamic);
        assertTrue(columnDefinitions.get(0).strict);

        assertThat(columnDefinitions.get(1).columnName, is("datum"));
        assertThat(columnDefinitions.get(1).dataType, is("timestamp"));
        assertThat(columnDefinitions.get(1).ordinalPosition, is(2));
        assertThat(columnDefinitions.get(1).tableName, is("test1"));
        assertFalse(columnDefinitions.get(1).dynamic);
        assertTrue(columnDefinitions.get(1).strict);

        assertThat(columnDefinitions.get(2).columnName, is("id"));
        assertThat(columnDefinitions.get(2).dataType, is("integer"));
        assertThat(columnDefinitions.get(2).ordinalPosition, is(3));
        assertThat(columnDefinitions.get(2).tableName, is("test1"));
        assertFalse(columnDefinitions.get(2).dynamic);
        assertTrue(columnDefinitions.get(2).strict);

        assertThat(columnDefinitions.get(3).columnName, is("nested"));
        assertThat(columnDefinitions.get(3).dataType, is("craty"));
        assertThat(columnDefinitions.get(3).ordinalPosition, is(4));
        assertThat(columnDefinitions.get(3).tableName, is("test1"));
        assertTrue(columnDefinitions.get(3).dynamic);
        assertFalse(columnDefinitions.get(3).strict);

        assertThat(columnDefinitions.get(4).columnName, is("nested.inner_nested"));
        assertThat(columnDefinitions.get(4).dataType, is("timestamp"));
        assertThat(columnDefinitions.get(4).ordinalPosition, is(5));
        assertThat(columnDefinitions.get(4).tableName, is("test1"));
        assertFalse(columnDefinitions.get(4).dynamic);
        assertTrue(columnDefinitions.get(4).strict);

        assertThat(columnDefinitions.get(5).columnName, is("person"));
        assertThat(columnDefinitions.get(5).dataType, is("craty"));
        assertThat(columnDefinitions.get(5).ordinalPosition, is(6));
        assertThat(columnDefinitions.get(5).tableName, is("test1"));
        assertTrue(columnDefinitions.get(5).dynamic);
        assertFalse(columnDefinitions.get(5).strict);

        assertThat(columnDefinitions.get(6).columnName, is("person.birthday"));
        assertThat(columnDefinitions.get(6).dataType, is("timestamp"));
        assertThat(columnDefinitions.get(6).ordinalPosition, is(7));
        assertThat(columnDefinitions.get(6).tableName, is("test1"));
        assertFalse(columnDefinitions.get(6).dynamic);
        assertTrue(columnDefinitions.get(6).strict);

        assertThat(columnDefinitions.get(7).columnName, is("person.first_name"));
        assertThat(columnDefinitions.get(7).dataType, is("string"));
        assertThat(columnDefinitions.get(7).ordinalPosition, is(8));
        assertThat(columnDefinitions.get(7).tableName, is("test1"));
        assertFalse(columnDefinitions.get(7).dynamic);
        assertTrue(columnDefinitions.get(7).strict);

        assertThat(columnDefinitions.get(8).columnName, is("title"));
        assertThat(columnDefinitions.get(8).dataType, is("string"));
        assertThat(columnDefinitions.get(8).ordinalPosition, is(9));
        assertThat(columnDefinitions.get(8).tableName, is("test1"));
        assertFalse(columnDefinitions.get(8).dynamic);
        assertTrue(columnDefinitions.get(8).strict);
    }

    @Test
    public void testExtractColumnDefinitionsFromEmptyIndex() throws Exception {
        node.client().admin().indices().prepareCreate("test2").execute().actionGet();
        refresh();
        IndexMetaData metaData = getIndexMetaData("test2");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();
        assertEquals(0, columnDefinitions.size());
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
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("title")
                                .field("type", "string")
                                .field("index", "no")
                            .endObject()
                            .startObject("datum")
                                .field("type", "date")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "analyzed")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test3")
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();
        IndexMetaData metaData = getIndexMetaData("test3");
        IndexMetaDataExtractor extractor1 = new IndexMetaDataExtractor(metaData);
        assertThat(extractor1.getPrimaryKeys().size(), is(1));
        assertThat(extractor1.getPrimaryKeys(), contains("id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("properties")
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "not_analyzed")

                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test4")
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();

        IndexMetaDataExtractor extractor2 = new IndexMetaDataExtractor(getIndexMetaData("test4"));
        assertThat(extractor2.getPrimaryKeys().size(), is(0));

        node.client().admin().indices().prepareCreate("test5").execute().actionGet();
        IndexMetaDataExtractor extractor3 = new IndexMetaDataExtractor(getIndexMetaData("test5"));
        assertThat(extractor3.getPrimaryKeys().size(), is(0));
    }

    @Test
    public void testGetIndices() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("properties")
                        .startObject("id")
                            .field("type", "integer")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("title")
                            .field("type", "multi_field")
                            .field("path", "just_name")
                            .startObject("fields")
                                .startObject("title")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                .endObject()
                                .startObject("ft")
                                    .field("type", "string")
                                    .field("index", "analyzed")
                                    .field("analyzer", "english")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("datum")
                            .field("type", "date")
                        .endObject()
                        .startObject("content")
                            .field("type", "multi_field")
                            .field("path", "just_name")
                            .startObject("fields")
                                .startObject("content")
                                    .field("type", "string")
                                    .field("index", "no")
                                .endObject()
                                .startObject("ft")
                                    .field("type", "string")
                                    .field("index", "analyzed")
                                    .field("analyzer", "english")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test6")
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();

        IndexMetaData metaData = getIndexMetaData("test6");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();

        assertEquals(5, indices.size());

        assertThat(indices.get(0).indexName, is("ft"));
        assertThat(indices.get(0).tableName, is("test6"));
        assertThat(indices.get(0).method, is("fulltext"));
        assertThat(indices.get(0).getColumnsString(), is("content, title"));
        assertThat(indices.get(0).getPropertiesString(), is("analyzer=english"));
        assertThat(indices.get(0).getUid(), is("test6.ft"));

        assertThat(indices.get(1).indexName, is("datum"));
        assertThat(indices.get(1).tableName, is("test6"));
        assertThat(indices.get(1).method, is("plain"));
        assertThat(indices.get(1).getColumnsString(), is("datum"));
        assertThat(indices.get(1).getUid(), is("test6.datum"));

        assertThat(indices.get(2).indexName, is("id"));
        assertThat(indices.get(2).tableName, is("test6"));
        assertThat(indices.get(2).getColumnsString(), is("id"));
        assertThat(indices.get(2).method, is("plain"));

        assertThat(indices.get(3).indexName, is("title"));
        assertThat(indices.get(3).tableName, is("test6"));
        assertThat(indices.get(3).getColumnsString(), is("title"));
        assertThat(indices.get(3).method, is("plain"));

        // compound indices will be listed once for every column they index,
        // will be merged by indicesTable
        assertThat(indices.get(4).indexName, is("ft"));
    }

    @Test
    public void extractIndicesFromEmptyIndex() throws Exception {
        node.client().admin().indices().prepareCreate("test7").execute().actionGet();
        refresh();

        IndexMetaData metaData = getIndexMetaData("test7");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();
        assertEquals(0, indices.size());

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
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("title")
                                .field("type", "multi_field")
                                .field("path", "just_name")
                                .startObject("fields")
                                    .startObject("title")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("ft")
                                        .field("type", "string")
                                        .field("index", "analyzed")
                                        .field("analyzer", "english")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("datum")
                                .field("type", "date")
                            .endObject()
                            .startObject("content")
                                .field("type", "multi_field")
                                .field("path", "just_name")
                                .startObject("fields")
                                    .startObject("content")
                                        .field("type", "string")
                                        .field("index", "no")
                                    .endObject()
                                    .startObject("ft")
                                        .field("type", "string")
                                        .field("index", "analyzed")
                                        .field("analyzer", "english")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test8")
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();

        IndexMetaData metaData = getIndexMetaData("test8");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("properties")
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test9")
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();

        metaData = getIndexMetaData("test9");
        extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("_id"));

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("_meta")
                            .field("primary_keys", "id")
                        .endObject()
                        .startObject("_routing")
                            .field("path", "id")
                        .endObject()
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                .field("index", "not_analyzed")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("index", "no")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test10")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();

        metaData = getIndexMetaData("test10");
        extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("id"));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        node.client().admin().indices().prepareCreate("test11").execute().actionGet();
        refresh();

        IndexMetaData metaData = getIndexMetaData("test11");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        assertThat(extractor.getRoutingColumn(), is("_id"));
    }

    @Test
    public void testExtractObjectColumn() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .startObject("properties")
                            .startObject("craty_field")
                                .startObject("properties")
                                    .startObject("size")
                                        .field("type", "byte")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("created")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("strict_field")
                                .field("type", "object")
                                .field("dynamic", "strict")
                                .startObject("properties")
                                    .startObject("path")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("created")
                                        .field("type", "date")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject("no_dynamic_field")
                                .field("type", "object")
                                .field("dynamic", false)
                                .startObject("properties")
                                    .startObject("path")
                                        .field("type", "string")
                                        .field("index", "not_analyzed")
                                    .endObject()
                                    .startObject("dynamic_again")
                                        .startObject("properties")
                                            .startObject("field")
                                                .field("type", "date")
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test12")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        refresh();
        IndexMetaData metaData = getIndexMetaData("test12");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<ColumnDefinition> columns = extractor.getColumnDefinitions();

        assertEquals(10, columns.size());

        assertThat(columns.get(0).columnName, is("craty_field"));
        assertThat(columns.get(0).dynamic, is(true));
        assertThat(columns.get(0).dataType, is("craty"));
        assertThat(columns.get(0).ordinalPosition, is(1));
        assertThat(columns.get(0).tableName, is("test12"));
        assertTrue(columns.get(0) instanceof ObjectColumnDefinition);
        ObjectColumnDefinition objectColumn = (ObjectColumnDefinition)columns.get(0);
        assertEquals(2, objectColumn.nestedColumns.size());

        assertEquals(columns.get(1), objectColumn.nestedColumns.get(0));
        assertThat(columns.get(1).columnName, is("craty_field.created"));
        assertThat(columns.get(1).dataType, is("timestamp"));
        assertThat(columns.get(1).ordinalPosition, is(2));
        assertFalse(columns.get(1).dynamic);
        assertThat(columns.get(1).tableName, is("test12"));

        assertEquals(columns.get(2), objectColumn.nestedColumns.get(1));
        assertThat(columns.get(2).columnName, is("craty_field.size"));
        assertThat(columns.get(2).dataType, is("byte"));
        assertThat(columns.get(2).ordinalPosition, is(3));
        assertFalse(columns.get(2).dynamic);
        assertThat(columns.get(2).tableName, is("test12"));

        assertThat(columns.get(3).columnName, is("no_dynamic_field"));
        assertThat(columns.get(3).dynamic, is(false));
        assertThat(columns.get(3).dataType, is("craty"));
        assertThat(columns.get(3).ordinalPosition, is(4));
        assertThat(columns.get(3).tableName, is("test12"));
        assertTrue(columns.get(3) instanceof ObjectColumnDefinition);
        objectColumn = (ObjectColumnDefinition)columns.get(3);
        assertEquals(3, objectColumn.nestedColumns.size());

        assertEquals(columns.get(4), objectColumn.nestedColumns.get(0));
        assertThat(columns.get(4).columnName, is("no_dynamic_field.dynamic_again"));
        assertThat(columns.get(4).dataType, is("craty"));
        assertThat(columns.get(4).ordinalPosition, is(5));
        assertTrue(columns.get(4).dynamic);
        assertThat(columns.get(4).tableName, is("test12"));

        assertEquals(columns.get(5), objectColumn.nestedColumns.get(1));
        assertThat(columns.get(5).columnName, is("no_dynamic_field.dynamic_again.field"));
        assertThat(columns.get(5).dataType, is("timestamp"));
        assertThat(columns.get(5).ordinalPosition, is(6));
        assertFalse(columns.get(5).dynamic);
        assertThat(columns.get(5).tableName, is("test12"));

        assertEquals(columns.get(6), objectColumn.nestedColumns.get(2));
        assertThat(columns.get(6).columnName, is("no_dynamic_field.path"));
        assertThat(columns.get(6).dataType, is("string"));
        assertThat(columns.get(6).ordinalPosition, is(7));
        assertFalse(columns.get(6).dynamic);
        assertThat(columns.get(6).tableName, is("test12"));

    }

    @Test
    public void testIsDynamic() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                    .startObject("field")
                        .field("type", "string")
                    .endObject()
                .endObject()
                .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test13")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2)
                        .put("index.mapper.dynamic", false))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        IndexMetaData metaData = getIndexMetaData("test13");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", true)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test14")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2)
                        .put("index.mapper.dynamic", false))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        metaData = getIndexMetaData("test13");
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(Constants.DEFAULT_MAPPING_TYPE)
                        .field("dynamic", false)
                        .startObject("properties")
                            .startObject("field")
                                .field("type", "string")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test15")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        metaData = getIndexMetaData("test15");
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test16")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        metaData = getIndexMetaData("test16");
        extractor = new IndexMetaDataExtractor(metaData);
        assertFalse(extractor.isDynamic());

        builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(Constants.DEFAULT_MAPPING_TYPE)
                .startObject("properties")
                .startObject("field")
                .field("type", "string")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        node.client().admin().indices().prepareCreate("test17")
                .setSettings(ImmutableSettings.builder()
                        .put("number_of_replicas", 0)
                        .put("number_of_shards", 2))
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, builder)
                .execute().actionGet();
        metaData = getIndexMetaData("test17");
        extractor = new IndexMetaDataExtractor(metaData);
        assertTrue(extractor.isDynamic());

    }
}
