package org.cratedb.module.sql.benchmark;

import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.cratedb.test.integration.PathAccessor.bytesFromPath;
import static org.cratedb.test.integration.PathAccessor.stringFromPath;


public class BenchmarkBase extends AbstractCrateNodesTests {

    public static final String NODE1 = "node1";
    public static final String NODE2 = "node2";
    public static final String INDEX_NAME = "countries";
    public static final String SETTINGS = "/essetup/settings/bench.json";
    public static final String MAPPING = "/essetup/mappings/bench.json";
    public static final String DATA = "/essetup/data/bench.json";
    public static List<Node> startedNodes = new ArrayList<>(2);

    @Before
    public void prepareBenchmarkRun() throws Exception {

        for (String nodeId : new String[]{NODE1, NODE2}) {
            Node insertNode = node(nodeId);
            if (insertNode==null) {
                startedNodes.add(startNode(nodeId, getNodeSettings(nodeId)));
            }
        }
        if (!indexExists()) {
            getClient(false).admin().indices().prepareCreate(INDEX_NAME).setSettings(
                    ImmutableSettings.builder().loadFromClasspath(SETTINGS).build())
                    .addMapping("default", stringFromPath(MAPPING, InsertBenchmark.class)).execute().actionGet();
            refresh(client());
            if (loadData()) {
                doLoadData();
            }
        }
    }

    @AfterClass
    public static void shutDownNodes() {
        for (Node node: startedNodes) {
            if (node != null && !node.isClosed()) {
                node.close();
            }
        }
        startedNodes.clear();
    }

    public boolean nodesStarted() {
        return node(NODE1) != null && node(NODE2) != null;
    }

    public boolean indexExists() {
        return getClient(false).admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists();
    }

    public boolean loadData() {
        return false;
    }

    public void doLoadData() throws Exception {
        loadBulk(DATA, false);
        refresh(getClient(true));
        refresh(getClient(false));
    }

    public Settings getNodeSettings(String nodeId) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("index.store.type", "memory");
        switch (nodeId) {
            case NODE1:
                builder.put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, true);
                break;
            case NODE2:
                builder.put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, false);
                break;
        }
        return builder.build();
    }

    public Client getClient(boolean queryPlannerEnabled) {
        return client(queryPlannerEnabled ? NODE1: NODE2);
    }

    public void loadBulk(String path, boolean queryPlannerEnabled) throws Exception {
        byte[] bulkPayload = bytesFromPath(path, this.getClass());
        BulkResponse bulk = getClient(queryPlannerEnabled).prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
    }
}
