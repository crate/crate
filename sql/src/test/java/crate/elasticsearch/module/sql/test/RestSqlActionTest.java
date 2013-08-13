package crate.elasticsearch.module.sql.test;

import com.akiban.sql.StandardException;
import crate.elasticsearch.action.SQLRequestBuilder;
import crate.elasticsearch.TestSetup;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.io.IOException;

public class RestSqlActionTest extends TestSetup {


    @Test
    public void testSqlRequest() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(13, response.getHits().getTotalHits());
    }

    @Test
    public void testSqlRequestWithLimit() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations limit 2\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(2, response.getHits().getHits().length);
    }

    @Test
    public void testSqlRequestWithLimitAndOffset() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations limit 15 offset 2\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        SearchHit[] hits = response.getHits().getHits();
        // because of the offset, only 9 instead of all 11 are displayed
        assertEquals(11, hits.length);
    }

    @Test
    public void testSqlRequestWithFilter() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations where kind = 'Planet'\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(5, response.getHits().getHits().length);
    }

    @Test
    public void testSqlRequestWithNotEqual() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations where kind != 'Planet'\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(8, response.getHits().getHits().length);
    }

    @Test
    public void testSqlRequestWithOneOrFilter() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations where kind = 'Planet' or kind = 'Galaxy'\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(9, response.getHits().getTotalHits());
    }

    @Test
    public void testSqlRequestWithDateFilter() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations where date = '2013-05-01'\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(1, response.getHits().getTotalHits());
    }

    @Test
    public void testSqlRequestWithDateGtFilter() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select name from locations where date > '2013-05-01'\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(8, response.getHits().getTotalHits());
    }

    @Test
    public void testSqlRequestWithNumericGtFilter() throws StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();
        builder.source("{\"stmt\": \"select * from locations where position > 3\"}");
        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(5, response.getHits().getTotalHits());
    }

    @Test
    public void testSqlRequestWithMultipleOr() throws IOException, StandardException {
        SQLRequestBuilder builder = new SQLRequestBuilder();

        builder.source(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("stmt", "select * from locations where \"_id\" = 1 or \"_id\" = 2 or \"_id\" = 3")
                .endObject()
                .string()
        );

        SearchRequest request = builder.buildSearchRequest();

        Client client = esSetup.client();
        ActionFuture<SearchResponse> responseFuture = client.execute(SearchAction.INSTANCE, request);
        SearchResponse response = responseFuture.actionGet();

        Throwable failure = responseFuture.getRootFailure();

        assertNull(failure);
        assertEquals(0, response.getFailedShards());
        assertEquals(3, response.getHits().getTotalHits());
    }
}
