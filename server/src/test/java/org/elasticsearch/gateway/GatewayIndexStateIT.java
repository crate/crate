/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.SQLTransportExecutor.REQUEST_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.IntegTestCase.Scope;
import org.elasticsearch.test.TestCluster.RestartCallback;
import org.junit.Test;

import io.crate.action.sql.SQLOperations;
import io.crate.common.unit.TimeValue;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.testing.SQLTransportExecutor;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class GatewayIndexStateIT extends IntegTestCase {

    private final Logger logger = LogManager.getLogger(GatewayIndexStateIT.class);

    @Override
    protected boolean addMockInternalEngine() {
        // testRecoverBrokenIndexMetadata replies on the flushing on shutdown behavior which can be randomly disabled in MockInternalEngine.
        return false;
    }

    public void testSimpleOpenClose() throws Exception {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> creating test index");

        int numPrimaries = 2;
        int numReplicas = 1;
        int totalNumShards = numPrimaries + (numPrimaries * numReplicas);
        var tableName = getFqn("test");

        execute("create table test (id int primary key) clustered into ? shards with (number_of_replicas = ?)",
                new Object[]{numPrimaries, numReplicas});

        logger.info("--> waiting for green status");
        ensureGreen();

        ClusterStateResponse stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index(tableName).shards().size(), equalTo(numPrimaries));
        assertThat(stateResponse.getState().routingTable().index(tableName).shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(totalNumShards));

        logger.info("--> insert a simple document");
        execute("insert into test (id) values (1)");

        logger.info("--> closing test index...");
        execute("alter table test close");

        stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index(tableName), notNullValue());

        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> trying to index into a closed index ...");
        try {
            execute("insert into test (id) values (2)");
            fail();
        } catch (Exception e) {
            // all is well
        }

        logger.info("--> creating another index (test2) and indexing into it");
        execute("create table test2 (id int primary key) with (number_of_replicas = 0)");
        execute("insert into test2 (id) values (1)");

        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> opening the first index again...");
        execute("alter table test open");

        logger.info("--> verifying that the state is green");
        ensureGreen();

        stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index(tableName).shards().size(), equalTo(numPrimaries));
        assertThat(stateResponse.getState().routingTable().index(tableName).shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(totalNumShards));

        logger.info("--> trying to get the indexed document on the first index");
        execute("select id from test where id = 1");
        assertThat(response.rowCount(), is(1L));


        logger.info("--> closing test index...");
        execute("alter table test close");

        stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index(tableName), notNullValue());

        logger.info("--> restarting nodes...");
        internalCluster().fullRestart();
        logger.info("--> waiting for two nodes and green status");
        ensureGreen();

        stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index(tableName), notNullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            execute("insert into test (id) values (2)");
            fail();
        } catch (Exception e) {
            // all is well
        }

        logger.info("--> opening index...");
        execute("alter table test open");

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = client().admin().cluster().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index(tableName).shards().size(), equalTo(numPrimaries));
        assertThat(stateResponse.getState().routingTable().index(tableName).shardsWithState(ShardRoutingState.STARTED).size(),
            equalTo(totalNumShards));

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        execute("select id from test where id = 1");
        assertThat(response.rowCount(), is(1L));

        logger.info("--> indexing a simple document");
        execute("insert into test (id) values (2)");
    }

    /**
     * Creating a table without any data node will take very long as internally at CrateDB, a table creation
     * is waiting for all shards to acknowledge until it times out if no data node is available.
     * So this will run under the @Slow annotation.
     */
    @Slow
    @Test
    public void testJustMasterNode() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());

        logger.info("--> create an index");
        execute("create table test (id int) with (number_of_replicas = 0, \"write.wait_for_active_shards\" = 0)",
                null,
                new TimeValue(90, TimeUnit.SECONDS));
        var tableName = getFqn("test");

        logger.info("--> restarting master node");
        internalCluster().fullRestart(new RestartCallback(){
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build();
            }
        });

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = FutureUtils.get(
            client().admin().cluster().health(
                new ClusterHealthRequest(tableName)
                    .waitForEvents(Priority.LANGUID)
            ),
            REQUEST_TIMEOUT
        );
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        var clusterStateResponse = client().admin().cluster().state(
            new ClusterStateRequest()
                .indices(tableName)
            ).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(clusterStateResponse.getState().metadata().hasIndex(tableName), equalTo(true));
    }

    @Test
    public void testJustMasterNodeAndJustDataNode() {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());
        internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build());

        logger.info("--> create an index");
        execute("create table test (id int) with (number_of_replicas = 0)");

        execute("insert into test (id) values (1)");
    }

    @Test
    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);

        logger.info("--> indexing a simple document");
        var tableName = getFqn("test");
        execute("create table test (id int) with (number_of_replicas = 0)");
        execute("insert into test (id) values (1)");
        execute("refresh table test");

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = FutureUtils.get(
            client().admin().cluster().health(
                new ClusterHealthRequest()
                    .waitForEvents(Priority.LANGUID)
                    .waitForGreenStatus()
                    .waitForNodes("2")
            ),
            REQUEST_TIMEOUT
        );
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            execute("select id from test");
            assertThat(response.rowCount(), is(1L));
        }

        logger.info("--> closing test index...");
        execute("alter table test close");

        ClusterStateResponse stateResponse = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(tableName).getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index(tableName), notNullValue());

        logger.info("--> opening the index...");
        execute("alter table test open");

        logger.info("--> waiting for green status");
        health = FutureUtils.get(
            client().admin().cluster().health(
                new ClusterHealthRequest()
                    .waitForEvents(Priority.LANGUID)
                    .waitForGreenStatus()
                    .waitForNodes("2")
            ),
            REQUEST_TIMEOUT
        );
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        execute("select id from test");
        assertThat(response.rowCount(), is(1L));
        for (int i = 0; i < 10; i++) {
            execute("select id from test");
            assertThat(response.rowCount(), is(1L));
        }
    }

    /**
     * This test ensures that when an index deletion takes place while a node is offline, when that
     * node rejoins the cluster, it deletes the index locally instead of importing it as a dangling index.
     */
    @Test
    public void testIndexDeletionWhenNodeRejoins() throws Exception {
        final int numNodes = 2;

        final List<String> nodes;
        logger.info("--> starting a cluster with " + numNodes + " nodes");
        nodes = internalCluster().startNodes(numNodes,
            Settings.builder().put(IndexGraveyard.SETTING_MAX_TOMBSTONES.getKey(), randomIntBetween(10, 100)).build());
        logger.info("--> create an index");
        //createIndex(indexName);
        execute("create table my_schema.test (id int) with (number_of_replicas = 0)");
        var tableName = "my_schema.test";


        logger.info("--> waiting for green status");
        ensureGreen();
        final String indexUUID = resolveIndex(tableName).getUUID();

        logger.info("--> restart a random date node, deleting the index in between stopping and restarting");
        internalCluster().restartRandomDataNode(new RestartCallback() {
            @Override
            public Settings onNodeStopped(final String nodeName) throws Exception {
                nodes.remove(nodeName);
                logger.info("--> stopped node[{}], remaining nodes {}", nodeName, nodes);
                assert nodes.size() > 0;
                final String otherNode = nodes.get(0);
                logger.info("--> delete index and verify it is deleted");
                var clientProvider = new SQLTransportExecutor.ClientProvider() {
                    @Override
                    public Client client() {
                        return IntegTestCase.client(otherNode);
                    }

                    @Override
                    public String pgUrl() {
                        PostgresNetty postgresNetty = internalCluster().getInstance(PostgresNetty.class, otherNode);
                        BoundTransportAddress boundTransportAddress = postgresNetty.boundAddress();
                        if (boundTransportAddress != null) {
                            InetSocketAddress address = boundTransportAddress.publishAddress().address();
                            return String.format(Locale.ENGLISH, "jdbc:postgresql://%s:%d/?ssl=%s&sslmode=%s",
                                                 address.getHostName(), address.getPort(), false, "disable");
                        }
                        return null;
                    }

                    @Override
                    public SQLOperations sqlOperations() {
                        return internalCluster().getInstance(SQLOperations.class, otherNode);
                    }
                };
                var sqlExecutor = new SQLTransportExecutor(clientProvider);
                //client.admin().indices().prepareDelete(indexName).execute().actionGet();
                sqlExecutor.exec("drop table my_schema.test");
                assertThat(response.rowCount(), is(1L));
                //assertFalse(indexExists(indexName, client));
                try {
                    sqlExecutor.exec("select * from my_schema.test");
                    fail("expecting index to be deleted");
                } catch (Exception e) {
                    // pass
                }
                logger.info("--> index deleted");
                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> wait until all nodes are back online");
        var clusterHealthRequest = new ClusterHealthRequest()
            .waitForEvents(Priority.LANGUID)
            .waitForNodes(Integer.toString(numNodes));
        FutureUtils.get(client().admin().cluster().health(clusterHealthRequest), REQUEST_TIMEOUT);

        logger.info("--> waiting for green status");
        ensureGreen();

        logger.info("--> verify that the deleted index is removed from the cluster and not reimported as dangling by the restarted node");
        //assertFalse(indexExists(indexName));
        try {
            execute("select * from my_schema.test");
            fail("expecting index to be deleted");
        } catch (Exception e) {
            // pass
        }
        assertBusy(() -> {
            final NodeEnvironment nodeEnv = internalCluster().getInstance(NodeEnvironment.class);
            try {
                assertFalse("index folder " + indexUUID + " should be deleted", nodeEnv.availableIndexFolders().contains(indexUUID));
            } catch (IOException e) {
                logger.error("Unable to retrieve available index folders from the node", e);
                fail("Unable to retrieve available index folders from the node");
            }
        });
    }

    /**
     * This test really tests worst case scenario where we have a broken setting or any setting that prevents an index from being
     * allocated in our metadata that we recover. In that case we now have the ability to check the index on local recovery from disk
     * if it is sane and if we can successfully create an IndexService. This also includes plugins etc.
     */
    @Test
    public void testRecoverBrokenIndexMetadata() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        logger.info("--> indexing a simple document");
        var tableName = getFqn("test");
        execute("create table test (id int) with (number_of_replicas = 0)");
        execute("insert into test (id) values (1)");
        execute("refresh table test");
        logger.info("--> waiting for green status");
        if (usually()) {
            ensureYellow();
        } else {
            internalCluster().startNode();
            FutureUtils.get(client().admin().cluster()
                .health(new ClusterHealthRequest()
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true).waitForNodes("2")), REQUEST_TIMEOUT);
        }
        ClusterState state = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();

        final IndexMetadata metadata = state.getMetadata().index(tableName);
        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).settings(Settings.builder().put(metadata.getSettings())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion().internalId)
            // this is invalid but should be archived
            .put("index.similarity.BM25.type", "classic")
            // this one is not validated ahead of time and breaks allocation
            .put("index.analysis.filter.myCollator.type", "icu_collation"));
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(brokenMeta)));

        // check that the cluster does not keep reallocating shards
        assertBusy(() -> {
            final RoutingTable routingTable = client().admin().cluster()
                .state(new ClusterStateRequest())
                .get()
                .getState().routingTable();
            final IndexRoutingTable indexRoutingTable = routingTable.index(tableName);
            assertNotNull(indexRoutingTable);
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                assertTrue(shardRoutingTable.primaryShard().unassigned());
                assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    shardRoutingTable.primaryShard().unassignedInfo().getLastAllocationStatus());
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().getNumFailedAllocations(), greaterThan(0));
            }
        }, 60, TimeUnit.SECONDS);
        execute("alter table test close");

        state = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();
        assertEquals(IndexMetadata.State.CLOSE, state.getMetadata().index(metadata.getIndex()).getState());
        assertEquals("classic", state.getMetadata().index(metadata.getIndex()).getSettings().get("archived.index.similarity.BM25.type"));
        // try to open it with the broken setting - fail again!
        assertThrowsMatches(
            () -> execute("alter table test open"),
            isSQLError(is("Failed to verify index " + metadata.getIndex().getName()),
                       INTERNAL_ERROR,
                       INTERNAL_SERVER_ERROR,
                       5000)
        );
    }

    /**
     * This test really tests worst case scenario where we have a missing analyzer setting.
     * In that case we now have the ability to check the index on local recovery from disk
     * if it is sane and if we can successfully create an IndexService.
     * This also includes plugins etc.
     */
    @Test
    public void testRecoverMissingAnalyzer() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        var tableName = getFqn("test");
        client().admin().indices().create(
            new CreateIndexRequest(
                tableName,
                Settings.builder()
                    .put("index.analysis.analyzer.test.tokenizer", "standard")
                    .put("index.number_of_shards", "1")
                    .build()
            )
            .mapping("{\n" +
                "    \"default\": {\n" +
                "      \"properties\": {\n" +
                "        \"field1\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"analyzer\": \"test\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }}", XContentType.JSON)
        ).get();
        logger.info("--> indexing a simple document");
        execute("insert into test (field1) values ('value one')");
        execute("refresh table test");

        logger.info("--> waiting for green status");
        if (usually()) {
            ensureYellow();
        } else {
            internalCluster().startNode();
            FutureUtils.get(client().admin().cluster()
                .health(new ClusterHealthRequest()
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true).waitForNodes("2")), REQUEST_TIMEOUT);
        }
        ClusterState state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();

        final IndexMetadata metadata = state.getMetadata().index(tableName);
        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).settings(metadata.getSettings()
                .filter((s) -> "index.analysis.analyzer.test.tokenizer".equals(s) == false));
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(brokenMeta)));

        // check that the cluster does not keep reallocating shards
        assertBusy(() -> {
            final RoutingTable routingTable = client().admin().cluster()
                .state(new ClusterStateRequest())
                .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS)
                .getState().routingTable();
            final IndexRoutingTable indexRoutingTable = routingTable.index(tableName);
            assertNotNull(indexRoutingTable);
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                assertTrue(shardRoutingTable.primaryShard().unassigned());
                assertEquals(UnassignedInfo.AllocationStatus.DECIDERS_NO,
                shardRoutingTable.primaryShard().unassignedInfo().getLastAllocationStatus());
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().getNumFailedAllocations(), greaterThan(0));
            }
        }, 60, TimeUnit.SECONDS);
        execute("alter table test close");

        // try to open it with the broken setting - fail again!
        assertThrowsMatches(
            () -> execute("alter table test open"),
            isSQLError(is("Failed to verify index " + metadata.getIndex().getName()),
                       INTERNAL_ERROR,
                       INTERNAL_SERVER_ERROR,
                       5000)
        );
    }

    @Test
    public void testArchiveBrokenClusterSettings() throws Exception {
        logger.info("--> starting one node");
        internalCluster().startNode();
        var tableName = getFqn("test");
        execute("create table test (id int) with (number_of_replicas = 0)");
        execute("insert into test (id) values (1)");
        execute("refresh table test");
        logger.info("--> waiting for green status");
        if (usually()) {
            ensureYellow();
        } else {
            internalCluster().startNode();
            FutureUtils.get(client().admin().cluster()
                .health(new ClusterHealthRequest()
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true).waitForNodes("2")), REQUEST_TIMEOUT);
        }
        ClusterState state = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();

        final Metadata metadata = state.getMetadata();
        final Metadata brokenMeta = Metadata.builder(metadata).persistentSettings(Settings.builder()
                    .put(metadata.persistentSettings()).put("this.is.unknown", true)
                    .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), "broken").build()).build();
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(brokenMeta));

        ensureYellow(tableName); // wait for state recovery
        state = client().admin().cluster()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();
        assertEquals("true", state.metadata().persistentSettings().get("archived.this.is.unknown"));
        assertEquals("broken", state.metadata().persistentSettings().get("archived."
            + SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()));

        // delete these settings
        client().admin().cluster().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest()
                .persistentSettings(Settings.builder().putNull("archived.*"))
        ).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        state = client().admin().cluster().state(new ClusterStateRequest()).get().getState();
        assertNull(state.metadata().persistentSettings().get("archived.this.is.unknown"));
        assertNull(state.metadata().persistentSettings().get("archived."
            + SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()));
        execute("select id from test");
        assertThat(response.rowCount(), is(1L));
    }

    private void restartNodesOnBrokenClusterState(ClusterState.Builder clusterStateBuilder) throws Exception {
        Map<String, PersistedClusterStateService> lucenePersistedStateFactories = Stream.of(internalCluster().getNodeNames())
            .collect(Collectors.toMap(Function.identity(), nodeName -> internalCluster().getInstance(PersistedClusterStateService.class, nodeName)));
        final ClusterState clusterState = clusterStateBuilder.build();
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                final PersistedClusterStateService lucenePersistedStateFactory = lucenePersistedStateFactories.get(
                    nodeName);
                try (PersistedClusterStateService.Writer writer = lucenePersistedStateFactory.createWriter()) {
                    writer.writeFullStateAndCommit(clusterState.term(), clusterState);
                }
                return super.onNodeStopped(nodeName);
            }
        });
    }
}
