package org.cratedb.module.sql.test;


import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.information_schema.IndexMetaDataExtractor;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.node.internal.InternalNode;
<<<<<<< HEAD
import org.junit.*;
=======
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
>>>>>>> master
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
<<<<<<< HEAD
            node.close();
        }
=======
            node.stop();
            node.close();
        }
        node = null;
>>>>>>> master
    }

    private SQLResponse execute(String stmt, Object[] args) {
        return node.client().execute(SQLAction.INSTANCE, new SQLRequest(stmt,
                args)).actionGet();
    }

    private SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }

<<<<<<< HEAD
=======
    private void refresh() {
        client().admin().indices().prepareRefresh().execute().actionGet();
    }

>>>>>>> master
    private IndexMetaData getIndexMetaData(String indexName) {
        ClusterStateResponse stateResponse = node.client().admin().cluster()
                .prepareState().execute().actionGet();
        return stateResponse.getState().metaData().indices().get(indexName);
    }

    @Test
    public void testExtractColumnDefinitions() throws Exception {
        execute("create table test1 (" +
                "id integer primary key, " +
                "title string index off, " +
                "datum timestamp, " +
                "content string index using fulltext" +
                ") replicas 0");
<<<<<<< HEAD
=======
        refresh();
>>>>>>> master
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
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaData metaData = getIndexMetaData("test2");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.ColumnDefinition> columnDefinitions = extractor.getColumnDefinitions();
        assertEquals(0, columnDefinitions.size());
    }

    @Test
    public void testExtractPrimaryKey() throws Exception {
        execute("create table test3 (" +
                "id integer primary key, " +
                "title string index off, " +
                "datum timestamp, " +
                "content string index using fulltext" +
                ") replicas 0");
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaData metaData = getIndexMetaData("test3");
        IndexMetaDataExtractor extractor1 = new IndexMetaDataExtractor(metaData);
        assertThat(extractor1.getPrimaryKey(), is("id"));

        execute("create table test4 (" +
                " content string" +
                ")");
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaDataExtractor extractor2 = new IndexMetaDataExtractor(getIndexMetaData("test4"));
        assertThat(extractor2.getPrimaryKey(), is(nullValue()));

        node.client().admin().indices().prepareCreate("test5").execute().actionGet();
        IndexMetaDataExtractor extractor3 = new IndexMetaDataExtractor(getIndexMetaData("test5"));
        assertThat(extractor3.getPrimaryKey(), is(nullValue()));
    }

    @Test
    public void testGetIndices() throws Exception {
        execute("create table test6 (" +
                "id integer primary key, " +
                "title string, " +
                "datum timestamp, " +
                "content string index off," +
                "index ft using fulltext(title, content) with (analyzer='english')" +
                ") replicas 0");
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaData metaData = getIndexMetaData("test6");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();

        assertEquals(5, indices.size());

        assertThat(indices.get(0).indexName, is("ft"));
        assertThat(indices.get(0).tableName, is("test6"));
        assertThat(indices.get(0).method, is("fulltext"));
        assertThat(indices.get(0).getExpressionsString(), is("content, title"));
        assertThat(indices.get(0).getPropertiesString(), is("analyzer=english"));
        assertThat(indices.get(0).getUid(), is("test6.ft"));

        assertThat(indices.get(1).indexName, is("datum"));
        assertThat(indices.get(1).tableName, is("test6"));
        assertThat(indices.get(1).method, is("plain"));
        assertThat(indices.get(1).getExpressionsString(), is("datum"));
        assertThat(indices.get(1).getUid(), is("test6.datum"));

        assertThat(indices.get(2).indexName, is("id"));
        assertThat(indices.get(2).tableName, is("test6"));
        assertThat(indices.get(2).getExpressionsString(), is("id"));
        assertThat(indices.get(2).method, is("plain"));

        assertThat(indices.get(3).indexName, is("title"));
        assertThat(indices.get(3).tableName, is("test6"));
        assertThat(indices.get(3).getExpressionsString(), is("title"));
        assertThat(indices.get(3).method, is("plain"));

        // compound indices will be listed once for every column they index,
        // will be merged by indicesTable
        assertThat(indices.get(4).indexName, is("ft"));
    }

    @Test
    public void extractIndicesFromEmptyIndex() throws Exception {
        node.client().admin().indices().prepareCreate("test7").execute().actionGet();
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaData metaData = getIndexMetaData("test7");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        List<IndexMetaDataExtractor.Index> indices = extractor.getIndices();
        assertEquals(0, indices.size());

    }

    @Test
    public void extractRoutingColumn() throws Exception {
        execute("create table test8 (" +
                "id integer primary key, " +
                "title string, " +
                "datum timestamp, " +
                "content string index off," +
                "index ft using fulltext(title, content) with (analyzer='english')" +
                ") replicas 0");
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaData metaData = getIndexMetaData("test8");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("id"));

        execute("create table test9 (content string) replicas 0");
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        metaData = getIndexMetaData("test9");
        extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("_id"));

        execute("create table test10 " +
                "(id integer primary key, content string) " +
                "clustered by(id) into 2 shards replicas 0");
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        metaData = getIndexMetaData("test10");
        extractor = new IndexMetaDataExtractor(metaData);

        assertThat(extractor.getRoutingColumn(), is("id"));
    }

    @Test
    public void extractRoutingColumnFromEmptyIndex() throws Exception {
        node.client().admin().indices().prepareCreate("test11").execute().actionGet();
<<<<<<< HEAD
=======
        refresh();

>>>>>>> master
        IndexMetaData metaData = getIndexMetaData("test11");
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(metaData);
        assertThat(extractor.getRoutingColumn(), is("_id"));
    }
}
