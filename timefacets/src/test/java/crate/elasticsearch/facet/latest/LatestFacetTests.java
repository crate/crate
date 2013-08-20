package crate.elasticsearch.facet.latest;


import crate.elasticsearch.facet.test.AbstractNodes;
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


public class LatestFacetTests extends AbstractNodes {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        // setup two nodes to have the transport protocol tested
        startNode("server1");
        startNode("server2");
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
        client.admin().indices().prepareDelete("data_1").execute().actionGet();
    }

    protected Client getClient() {
        return client("server1");
    }

    public void setupTemplates(Client client) throws Exception {
        String settings = XContentFactory.jsonBuilder().startObject()
                .field("number_of_shards", 2).field("number_of_replicas", 0)
                .startArray("aliases").value("data").endArray().endObject()
                .string();

        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("data").startObject("_all")
                .field("enabled", false).endObject().startObject("_source")
                .field("enabled", false).endObject().startObject("_routing")
                .field("required", true).field("store", false)
                .field("index", "not_analyzed").endObject()
                .startObject("properties").startObject("t")
                .field("type", "date").field("store", "yes").endObject()
                .startObject("k").field("type", "long").field("store", "yes")
                .endObject().startObject("v").field("type", "integer")
                .field("store", "yes").endObject().endObject().endObject()
                .endObject().string();
        client.admin().indices().preparePutTemplate("data")
                .setTemplate("data_*").setSettings(settings)
                .addMapping("data", mapping).execute().actionGet();
        Thread.sleep(100); // sleep a bit here..., so the mappings get applied
    }

    public void flush() {
        client.admin().indices().prepareFlush().setRefresh(true).execute()
                .actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void testCollapsing() throws Exception {
        for (int i = 0; i < 25; i++) {
            for (int ii = 10; ii < 100; ii += 10) {
                client.prepareIndex("data_1", "data")
                        .setRouting(Integer.toString(i))
                        .setSource(
                                XContentFactory.jsonBuilder().startObject()
                                        .field("t", ii * 10000 + i)
                                        .field("k", i).field("v", i * 10 + ii)
                                        .endObject()).execute().actionGet();
            }
        }

        flush();
        XContentBuilder facetQuery = XContentFactory
                .contentBuilder(XContentType.JSON).startObject()
                .startObject("facetname").startObject("latest")
                .field("size", 5).field("start", 2).field("key_field", "k")
                .field("value_field", "v").field("ts_field", "t").endObject()
                .endObject().endObject();
        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(facetQuery.bytes()).execute().actionGet();
        InternalLatestFacet facet = (InternalLatestFacet) response.getFacets()
                .facet("facetname");
        String expected = "{\"facetname\":{\"_type\":\"latest\",\"total\":25,"
                + "\"entries\":[" + "{\"value\":310,\"key\":22,\"ts\":900022},"
                + "{\"value\":300,\"key\":21,\"ts\":900021},"
                + "{\"value\":290,\"key\":20,\"ts\":900020},"
                + "{\"value\":280,\"key\":19,\"ts\":900019},"
                + "{\"value\":270,\"key\":18,\"ts\":900018}]}}";

        XContentBuilder builder = XContentFactory.contentBuilder(
                XContentType.JSON).startObject();
        facet.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();

        assertThat(builder.string(), equalTo(expected));

    }

}
