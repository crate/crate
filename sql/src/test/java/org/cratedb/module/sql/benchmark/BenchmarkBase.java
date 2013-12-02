package org.cratedb.module.sql.benchmark;

import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
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


@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 0, transportClientRatio = 0)
public class BenchmarkBase extends CrateIntegrationTest {

    public String NODE1 = null;
    public String NODE2 = null;
    public static final String INDEX_NAME = "countries";
    public static final String SETTINGS = "/essetup/settings/bench.json";
    public static final String MAPPING = "/essetup/mappings/bench.json";
    public static final String DATA = "/essetup/data/bench.json";

    @Before
    public void prepareBenchmarkRun() throws Exception {

        // TODO: start node global in static beforeClass

        NODE1 = cluster().startNode(getNodeSettings(1));
        NODE2 = cluster().startNode(getNodeSettings(2));

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

    public boolean nodesStarted() {
        return NODE1 != null && NODE2 != null;
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

    public Settings getNodeSettings(int nodeId) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put("index.store.type", "memory");
        switch (nodeId) {
            case 1:
                builder.put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, true);
                break;
            case 2:
                builder.put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, false);
                break;
        }
        return builder.build();
    }

    public Client getClient(boolean queryPlannerEnabled) {
        return queryPlannerEnabled ? cluster().client(NODE1) : cluster().client(NODE2);
    }

    public void loadBulk(String path, boolean queryPlannerEnabled) throws Exception {
        byte[] bulkPayload = bytesFromPath(path, this.getClass());
        BulkResponse bulk = getClient(queryPlannerEnabled).prepareBulk().add(bulkPayload, 0, bulkPayload.length, false, null, null).execute().actionGet();
        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : String.format("unable to index data {}", item);
        }
    }
}
