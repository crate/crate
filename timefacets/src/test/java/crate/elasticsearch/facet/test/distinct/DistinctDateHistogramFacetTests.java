package crate.elasticsearch.facet.test.distinct;


import crate.elasticsearch.facet.distinct.InternalDistinctDateHistogramFacet;
import crate.elasticsearch.facet.test.AbstractNodes;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DistinctDateHistogramFacetTests extends AbstractNodes {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        // setup two nodes to have the transport protocol tested
        startNode("server1");
        startNode("server2");
        startNode("server3");
        startNode("server4");
        client = getClient();
        setupTemplates(client);
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @AfterMethod
    public void tearDownData() {
        client.admin().indices().prepareDelete("distinct_data_1").execute().actionGet();
    }

    protected Client getClient() {
        return client("server1");
    }

    public void setupTemplates(Client client) throws Exception {
        String settings = XContentFactory.jsonBuilder()
                .startObject()
                .field("number_of_shards", 2)
                .field("number_of_replicas", 0)
                .startArray("aliases").value("data").endArray()
                .endObject().string();

        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("distinct_data")
                .startObject("_all").field("enabled", false).endObject()
                .startObject("_source").field("enabled", false).endObject()
                .startObject("properties")
                .startObject("created_at").field("type", "date").field("store", "yes").endObject()
                .startObject("distinct").field("type", "string").field("store", "yes").endObject()
                .startObject("wrong_type").field("type", "float").field("store", "yes").endObject()
                .startObject("long_type").field("type", "long").field("store", "yes").endObject()
                .endObject()
                .endObject()
                .endObject().string();
        client.admin().indices().preparePutTemplate("data")
                .setTemplate("data_*")
                .setSettings(settings)
                .addMapping("data", mapping)
                .execute().actionGet();
        Thread.sleep(100); // sleep a bit here..., so the mappings get applied
    }

    public void flush() {
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
        try {
            Thread.sleep(300); // sleep a bit here...
        } catch (InterruptedException e) {
        }
    }

    @Test(expectedExceptions = SearchPhaseExecutionException.class)
    public void testWrongKeyType() throws Exception {
        client.prepareIndex("distinct_data_1", "data", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("distinct", "1")
                        .field("wrong_type", 1.1)
                        .endObject())
                .execute().actionGet();
        flush();
        XContentBuilder facetQuery = XContentFactory.contentBuilder(XContentType.JSON)
                .startObject()
                .startObject("distinct")
                .startObject("distinct_date_histogram")
                .field("field", "distinct")
                .field("value_field", "wrong_type")
                .field("interval", "week")
                .endObject()
                .endObject()
                .endObject();

        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
    }

    @Test(expectedExceptions = SearchPhaseExecutionException.class)
    public void testWrongValueType() throws Exception {
        client.prepareIndex("distinct_data_1", "data", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("distinct", "1")
                        .field("wrong_type", 1.1)
                        .endObject())
                .execute().actionGet();
        flush();
        XContentBuilder facetQuery = XContentFactory.contentBuilder(XContentType.JSON)
                .startObject()
                .startObject("distinct")
                .startObject("distinct_date_histogram")
                .field("field", "created_at")
                .field("value_field", "wrong_type")
                .field("interval", "week")
                .endObject()
                .endObject()
                .endObject();

        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
    }

    @Test
    public void testDistinctString() throws Exception {
        client.prepareIndex("distinct_data_1", "data", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("distinct", "1")
                        .field("wrong_type", 1.1)
                        .endObject())
                .execute().actionGet();
        flush();
        XContentBuilder facetQuery = XContentFactory.contentBuilder(XContentType.JSON)
                .startObject()
                .startObject("distinct")
                .startObject("distinct_date_histogram")
                .field("field", "created_at")
                .field("value_field", "distinct")
                .field("interval", "week")
                .endObject()
                .endObject()
                .endObject();
        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
        InternalDistinctDateHistogramFacet facet = (InternalDistinctDateHistogramFacet) response.getFacets().facet("distinct");
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"distinct\":{" +
                        "\"_type\":\"distinct_date_histogram\",\"entries\":[" +
                        "{\"time\":950400000,\"count\":1}" +
                        "]," +
                        "\"count\":1" +
                        "}" +
                        "}"));

        client.prepareIndex("distinct_data_1", "data", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("distinct", "2")
                        .field("wrong_type", 1.1)
                        .endObject())
                .execute().actionGet();
        flush();
        response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
        facet = (InternalDistinctDateHistogramFacet) response.getFacets().facet("distinct");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"distinct\":{" +
                        "\"_type\":\"distinct_date_histogram\",\"entries\":[" +
                        "{\"time\":950400000,\"count\":2}" +
                        "]," +
                        "\"count\":2" +
                        "}" +
                        "}"));

        client.prepareIndex("distinct_data_1", "data", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .startArray("distinct")
                        .value("2")
                        .value("3")
                        .endArray()
                        .field("wrong_type", 1.1)
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("distinct_data_1", "data", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .startArray("distinct")
                        .value("3")
                        .value("4")
                        .endArray()
                        .field("wrong_type", 1.1)
                        .endObject())
                .execute().actionGet();
        flush();
        response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
        facet = (InternalDistinctDateHistogramFacet) response.getFacets().facet("distinct");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"distinct\":{" +
                        "\"_type\":\"distinct_date_histogram\",\"entries\":[" +
                        "{\"time\":950400000,\"count\":2}," +
                        "{\"time\":1555200000,\"count\":3}" +
                        "]," +
                        "\"count\":4" +
                        "}" +
                        "}"));
    }

    @Test
    public void testDistinctLong() throws Exception {
        client.prepareIndex("distinct_data_1", "data", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("long_type", 1)
                        .endObject())
                .execute().actionGet();
        flush();
        XContentBuilder facetQuery = XContentFactory.contentBuilder(XContentType.JSON)
                .startObject()
                .startObject("distinct")
                .startObject("distinct_date_histogram")
                .field("field", "created_at")
                .field("value_field", "long_type")
                .field("interval", "week")
                .endObject()
                .endObject()
                .endObject();
        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
        InternalDistinctDateHistogramFacet facet = (InternalDistinctDateHistogramFacet) response.getFacets().facet("distinct");
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"distinct\":{" +
                        "\"_type\":\"distinct_date_histogram\",\"entries\":[" +
                        "{\"time\":950400000,\"count\":1}" +
                        "]," +
                        "\"count\":1" +
                        "}" +
                        "}"));

        client.prepareIndex("distinct_data_1", "data", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 1000000000)
                        .field("long_type", 2)
                        .endObject())
                .execute().actionGet();
        flush();
        response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
        facet = (InternalDistinctDateHistogramFacet) response.getFacets().facet("distinct");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"distinct\":{" +
                        "\"_type\":\"distinct_date_histogram\",\"entries\":[" +
                        "{\"time\":950400000,\"count\":2}" +
                        "]," +
                        "\"count\":2" +
                        "}" +
                        "}"));

        client.prepareIndex("distinct_data_1", "data", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .startArray("long_type")
                        .value(2)
                        .value(3)
                        .endArray()
                        .endObject())
                .execute().actionGet();
        client.prepareIndex("distinct_data_1", "data", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("created_at", 2000000000)
                        .startArray("long_type")
                        .value(3)
                        .value(4)
                        .endArray()
                        .endObject())
                .execute().actionGet();
        flush();
        response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes().array())
                .execute().actionGet();
        facet = (InternalDistinctDateHistogramFacet) response.getFacets().facet("distinct");
        builder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        assertThat(builder.string(), equalTo(
                "{\"distinct\":{" +
                        "\"_type\":\"distinct_date_histogram\",\"entries\":[" +
                        "{\"time\":950400000,\"count\":2}," +
                        "{\"time\":1555200000,\"count\":3}" +
                        "]," +
                        "\"count\":4" +
                        "}" +
                        "}"));
    }
}
