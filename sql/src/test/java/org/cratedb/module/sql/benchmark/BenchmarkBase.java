package org.cratedb.module.sql.benchmark;

import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
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
    public void prepareIndex() throws Exception {
        for (String nodeId : new String[]{NODE1, NODE2}) {
            Node insertNode = node(nodeId);
            if (insertNode==null) {
                startedNodes.add(startNode(nodeId, getNodeSettings(nodeId)));
            }
        }
        if (!indexExists()) {
            client().admin().indices().prepareCreate(INDEX_NAME).setSettings(
                    ImmutableSettings.builder().loadFromClasspath(SETTINGS).build())
                    .addMapping("default", stringFromPath(MAPPING, InsertBenchmark.class)).execute().actionGet();

            if (loadData()) {
                loadBulk(DATA);
                client().admin().indices().refresh(new RefreshRequest(INDEX_NAME)).actionGet();
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
        return client().admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists();
    }

    public boolean loadData() {
        return false;
    }

    public Settings getNodeSettings(String nodeId) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("network.host", "127.0.0.1");
        switch (nodeId) {
            case NODE1:
                builder.put("transport.tcp.port", 9301);
                builder.put("http.port", 9201);
            case NODE2:
                builder.put("transport.tcp.port", 9402);
                builder.put("http.port", 9202);
        }
        return builder.build();
    }

    // TODO: copy & paste from AbstractSharedClusterTest
    public BulkResponse loadBulk(String path) throws Exception {
        byte[] bulkPayload = bytesFromPath(path, this.getClass());
        BulkResponse bulk = client().prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
        return bulk;
    }
}
