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

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
        node.client().admin().indices().prepareRefresh().execute().actionGet();
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
        List<IndexMetaDataExtractor.ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();

        assertEquals(4, columnDefinitions.size());

        assertThat(columnDefinitions.get(0).columnName, is("content"));
        assertThat(columnDefinitions.get(0).dataType, is("string"));
        assertThat(columnDefinitions.get(0).ordinalPosition, is(1));
        assertThat(columnDefinitions.get(0).tableName, is("test1"));

        assertThat(columnDefinitions.get(1).columnName, is("datum"));
        assertThat(columnDefinitions.get(1).dataType, is("timestamp"));
        assertThat(columnDefinitions.get(1).ordinalPosition, is(2));
        assertThat(columnDefinitions.get(1).tableName, is("test1"));

        assertThat(columnDefinitions.get(2).columnName, is("id"));
        assertThat(columnDefinitions.get(2).dataType, is("integer"));
        assertThat(columnDefinitions.get(2).ordinalPosition, is(3));
        assertThat(columnDefinitions.get(2).tableName, is("test1"));


        assertThat(columnDefinitions.get(3).columnName, is("title"));
        assertThat(columnDefinitions.get(3).dataType, is("string"));
        assertThat(columnDefinitions.get(3).ordinalPosition, is(4));
        assertThat(columnDefinitions.get(3).tableName, is("test1"));
    }

    @Test
    public void testExtractColumnDefinitionsFromEmptyIndex() throws Exception {
        node.client().admin().indices().prepareCreate("test2").execute().actionGet();
        refresh();
        IndexMetaData metaData = getIndexMetaData("test2");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();
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
        assertThat(extractor1.getPrimaryKey(), is("id"));

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
        assertThat(extractor2.getPrimaryKey(), is(nullValue()));

        node.client().admin().indices().prepareCreate("test5").execute().actionGet();
        IndexMetaDataExtractor extractor3 = new IndexMetaDataExtractor(getIndexMetaData("test5"));
        assertThat(extractor3.getPrimaryKey(), is(nullValue()));
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
}
