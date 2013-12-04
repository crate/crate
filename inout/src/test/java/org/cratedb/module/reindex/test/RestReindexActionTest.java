package org.cratedb.module.reindex.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import org.cratedb.action.reindex.ReindexAction;
import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.cratedb.module.AbstractRestActionTest;

public class RestReindexActionTest extends AbstractRestActionTest {

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder().put("number_of_shards", 1).build();
    }

    @Test
    public void testSearchIntoWithoutSource() {
        prepareCreate("test").addMapping("a",
            "{\"a\":{\"_source\": {\"enabled\": false}}}").execute().actionGet();
        client().index(new IndexRequest("test", "a", "1").source("{\"name\": \"John\"}")).actionGet();
        SearchIntoRequest request = new SearchIntoRequest("test");
        SearchIntoResponse res = client().execute(ReindexAction.INSTANCE, request).actionGet();
        assertEquals(1, res.getFailedShards());
        assertTrue(res.getShardFailures()[0].reason().contains("Parse Failure [The _source field of index test and type a is not stored.]"));
    }
}
