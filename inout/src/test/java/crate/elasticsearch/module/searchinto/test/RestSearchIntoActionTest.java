package crate.elasticsearch.module.searchinto.test;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.junit.Test;

import crate.elasticsearch.action.searchinto.SearchIntoAction;
import crate.elasticsearch.action.searchinto.SearchIntoRequest;
import crate.elasticsearch.action.searchinto.SearchIntoResponse;
import crate.elasticsearch.module.AbstractRestActionTest;

public class RestSearchIntoActionTest extends AbstractRestActionTest {

    @Test
    public void testSearchIntoWithoutSource() {
        esSetup.execute(createIndex("test").withMapping("a",
                "{\"a\":{\"_source\": {\"enabled\": false}}}"));
        esSetup.execute(index("test", "a", "1").withSource("{\"name\": \"John\"}"));
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", \"_source\", [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = esSetup.client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertEquals(1, res.getFailedShards());
        assertTrue(res.getShardFailures()[0].reason().contains("Parse Failure [The _source field of index test and type a is not stored.]"));
    }

    @Test
    public void testNestedObjectsRewriting() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x.city\", \"_source.city\"], [\"x.surname\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = esSetup.client().execute(SearchIntoAction.INSTANCE, request).actionGet();

        GetRequestBuilder rb = new GetRequestBuilder(esSetup.client(), "newindex");
        GetResponse getRes = rb.setType("a").setId("1").execute().actionGet();
        assertTrue(getRes.isExists());
        assertEquals("{\"x\":{\"name\":\"Doe\",\"surname\":\"John\",\"city\":\"Dornbirn\"}}", getRes.getSourceAsString());
    }

    @Test
    public void testNestedObjectsRewritingMixed1() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x\", \"_source.city\"], [\"x.surname\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = esSetup.client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertTrue(res.getShardFailures()[0].reason().contains("Error on rewriting objects: Mixed objects and values]"));
    }

    @Test
    public void testNestedObjectsRewritingMixed2() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x.surname.bad\", \"_source.city\"], [\"x.surname\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = esSetup.client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertTrue(res.getShardFailures()[0].reason().contains("Error on rewriting objects: Mixed objects and values]"));
    }

    @Test
    public void testNestedObjectsRewritingMixed3() {
        prepareNested();
        SearchIntoRequest request = new SearchIntoRequest("test");
        request.source("{\"fields\": [\"_id\", [\"x.surname\", \"_source.city\"], [\"x.surname.bad\", \"_source.name.surname\"], [\"x.name\", \"_source.name.name\"], [\"_index\", \"'newindex'\"]]}");
        SearchIntoResponse res = esSetup.client().execute(SearchIntoAction.INSTANCE, request).actionGet();
        assertTrue(res.getShardFailures()[0].reason().contains("Error on rewriting objects: Mixed objects and values]"));
    }


    private void prepareNested() {
        esSetup.execute(createIndex("test").withMapping("a",
                "{\"a\": {\"properties\": {\"name\": {\"properties\": {\"surname\":{\"type\":\"string\"}, \"name\": {\"type\":\"string\"}}}, \"city\": {\"type\": \"string\"}}}}"));
        esSetup.execute(index("test", "a", "1").withSource(
                "{\"city\": \"Dornbirn\", \"name\": {\"surname\": \"John\", \"name\": \"Doe\"}}"));
    }

    private static List<Map<String, Object>> get(SearchIntoResponse resp, String key) {
        Map<String, Object> res = null;
        try {
            res = toMap(resp);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return (List<Map<String, Object>>) res.get(key);
    }

}
