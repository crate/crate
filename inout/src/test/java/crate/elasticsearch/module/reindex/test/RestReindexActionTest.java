package crate.elasticsearch.module.reindex.test;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import crate.elasticsearch.action.reindex.ReindexAction;
import crate.elasticsearch.action.searchinto.SearchIntoRequest;
import crate.elasticsearch.action.searchinto.SearchIntoResponse;
import crate.elasticsearch.module.AbstractRestActionTest;

public class RestReindexActionTest extends AbstractRestActionTest {

    @Test
    public void testSearchIntoWithoutSource() {
        esSetup.execute(createIndex("test").withMapping("a",
                "{\"a\":{\"_source\": {\"enabled\": false}}}"));
        esSetup.execute(index("test", "a", "1").withSource("{\"name\": \"John\"}"));
        SearchIntoRequest request = new SearchIntoRequest("test");
        SearchIntoResponse res = esSetup.client().execute(ReindexAction.INSTANCE, request).actionGet();
        assertEquals(1, res.getFailedShards());
        assertTrue(res.getShardFailures()[0].reason().contains("Parse Failure [The _source field of index test and type a is not stored.]"));
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
