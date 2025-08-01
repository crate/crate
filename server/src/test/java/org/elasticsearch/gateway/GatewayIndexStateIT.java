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
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.SQLTransportExecutor.REQUEST_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;

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
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.IntegTestCase.ClusterScope;
import org.elasticsearch.test.IntegTestCase.Scope;
import org.elasticsearch.test.TestCluster.RestartCallback;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.session.Sessions;
import io.crate.testing.Asserts;
import io.crate.testing.SQLTransportExecutor;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class GatewayIndexStateIT extends IntegTestCase {

    private final Logger logger = LogManager.getLogger(GatewayIndexStateIT.class);

    @Override
    protected boolean addMockInternalEngine() {
        // testRecoverBrokenIndexMetadata replies on the flushing on shutdown behavior which can be randomly disabled in MockInternalEngine.
        return false;
    }

    @Test
    public void testSimpleOpenClose() throws Exception {
        logger.info("--> starting 2 nodes");
        cluster().startNodes(2);

        logger.info("--> creating test index");

        int numPrimaries = 2;
        int numReplicas = 1;
        int totalNumShards = numPrimaries + (numPrimaries * numReplicas);

        execute("create table test (id int primary key) clustered into ? shards with (number_of_replicas = ?)",
                new Object[]{numPrimaries, numReplicas});

        logger.info("--> waiting for green status");
        ensureGreen();

        String indexUUID = resolveIndex("test").getUUID();

        ClusterStateResponse stateResponse = client().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata().index(indexUUID).getState()).isEqualTo(IndexMetadata.State.OPEN);
        assertThat(stateResponse.getState().routingTable().index(indexUUID).shards()).hasSize(numPrimaries);
        assertThat(stateResponse.getState().routingTable().index(indexUUID).shardsWithState(ShardRoutingState.STARTED)).hasSize(totalNumShards);

        logger.info("--> insert a simple document");
        execute("insert into test (id) values (1)");

        logger.info("--> closing test index...");
        execute("alter table test close");

        stateResponse = client().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata().index(indexUUID).getState()).isEqualTo(IndexMetadata.State.CLOSE);
        assertThat(stateResponse.getState().routingTable().index(indexUUID)).isNotNull();

        logger.info("--> verifying that the state is green");
        ensureGreen();

        logger.info("--> trying to index into a closed index ...");
        try {
            execute("insert into test (id) values (2)");
            fail();
        } catch (Exception _) {
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

        stateResponse = client().state(new ClusterStateRequest()).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(indexUUID).getState()).isEqualTo(IndexMetadata.State.OPEN);
        assertThat(stateResponse.getState().routingTable().index(indexUUID).shards()).hasSize(numPrimaries);
        assertThat(stateResponse.getState().routingTable().index(indexUUID).shardsWithState(ShardRoutingState.STARTED)).hasSize(totalNumShards);

        logger.info("--> trying to get the indexed document on the first index");
        execute("select id from test where id = 1");
        assertThat(response.rowCount()).isEqualTo(1L);


        logger.info("--> closing test index...");
        execute("alter table test close");

        stateResponse = client().state(new ClusterStateRequest()).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(indexUUID).getState()).isEqualTo(IndexMetadata.State.CLOSE);
        assertThat(stateResponse.getState().routingTable().index(indexUUID)).isNotNull();

        logger.info("--> restarting nodes...");
        cluster().fullRestart();
        logger.info("--> waiting for two nodes and green status");
        ensureGreen();

        stateResponse = client().state(new ClusterStateRequest()).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertThat(stateResponse.getState().metadata().index(indexUUID).getState()).isEqualTo(IndexMetadata.State.CLOSE);
        assertThat(stateResponse.getState().routingTable().index(indexUUID)).isNotNull();

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

        stateResponse = client().state(new ClusterStateRequest()).get();
        assertThat(stateResponse.getState().metadata().index(indexUUID).getState()).isEqualTo(IndexMetadata.State.OPEN);
        assertThat(stateResponse.getState().routingTable().index(indexUUID).shards()).hasSize(numPrimaries);
        assertThat(stateResponse.getState().routingTable().index(indexUUID).shardsWithState(ShardRoutingState.STARTED)).hasSize(totalNumShards);

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        execute("select id from test where id = 1");
        assertThat(response.rowCount()).isEqualTo(1L);

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
        cluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());

        logger.info("--> create an index");
        execute("create table test (id int) with (number_of_replicas = 0, \"write.wait_for_active_shards\" = 0)",
                null,
                new TimeValue(90, TimeUnit.SECONDS));

        logger.info("--> restarting master node");
        cluster().fullRestart(new RestartCallback(){
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build();
            }
        });

        logger.info("--> verify we have an index");
        assertBusy(() -> {
            execute("select health from sys.health where table_name = 'test'");
            assertThat(response).hasRows("RED"); // no data nodes, no allocated shards
        });
    }

    @Test
    public void testJustMasterNodeAndJustDataNode() {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 1 master node non data");
        cluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build());
        cluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).build());

        logger.info("--> create an index");
        execute("create table test (id int) with (number_of_replicas = 0)");

        execute("insert into test (id) values (1)");
    }

    @Test
    public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");

        logger.info("--> starting 2 nodes");
        cluster().startNodes(2);

        logger.info("--> indexing a simple document");
        execute("create table test (id int) with (number_of_replicas = 0)");
        execute("insert into test (id) values (1)");
        execute("refresh table test");

        logger.info("--> waiting for green status");
        assertBusy(() -> {
            execute("select health from sys.health where table_name = 'test'");
            assertThat(response).hasRows("GREEN");
        });

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            execute("select id from test");
            assertThat(response.rowCount()).isEqualTo(1L);
        }

        logger.info("--> closing test index...");
        execute("alter table test close");

        execute("select closed from information_schema.tables where table_name = 'test'");
        assertThat(response).hasRows("true");

        assertThatThrownBy(() -> execute("select * from test"))
            .hasMessageContaining("doesn't support or allow READ operations, as it is currently closed");

        logger.info("--> opening the index...");
        execute("alter table test open");

        logger.info("--> waiting for green status");
        assertBusy(() -> {
            execute("select health from sys.health where table_name = 'test'");
            assertThat(response).hasRows("GREEN");
        });

        logger.info("--> verify 1 doc in the index");
        execute("select id from test");
        assertThat(response.rowCount()).isEqualTo(1L);
        for (int i = 0; i < 10; i++) {
            execute("select id from test");
            assertThat(response.rowCount()).isEqualTo(1L);
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
        nodes = cluster().startNodes(numNodes,
            Settings.builder().put(IndexGraveyard.SETTING_MAX_TOMBSTONES.getKey(), randomIntBetween(10, 100)).build());
        logger.info("--> create an index");
        execute("create table my_schema.test (id int) with (number_of_replicas = 0)");
        var tableName = "my_schema.test";


        logger.info("--> waiting for green status");
        ensureGreen();
        final String indexUUID = resolveIndex(tableName).getUUID();

        logger.info("--> restart a random date node, deleting the index in between stopping and restarting");
        cluster().restartRandomDataNode(new RestartCallback() {
            @Override
            public Settings onNodeStopped(final String nodeName) throws Exception {
                nodes.remove(nodeName);
                logger.info("--> stopped node[{}], remaining nodes {}", nodeName, nodes);
                assert nodes.size() > 0;
                final String otherNode = nodes.getFirst();
                logger.info("--> delete index and verify it is deleted");
                var clientProvider = new SQLTransportExecutor.ClientProvider() {
                    @Override
                    public Client client() {
                        return IntegTestCase.client(otherNode);
                    }

                    @Override
                    public String pgUrl() {
                        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class, otherNode);
                        BoundTransportAddress boundTransportAddress = postgresNetty.boundAddress();
                        if (boundTransportAddress != null) {
                            InetSocketAddress address = boundTransportAddress.publishAddress().address();
                            return String.format(Locale.ENGLISH, "jdbc:postgresql://%s:%d/?ssl=%s&sslmode=%s",
                                                 address.getHostName(), address.getPort(), false, "disable");
                        }
                        return null;
                    }

                    @Override
                    public Sessions sessions() {
                        return cluster().getInstance(Sessions.class, otherNode);
                    }
                };
                var sqlExecutor = new SQLTransportExecutor(clientProvider);
                sqlExecutor.exec("drop table my_schema.test");
                assertThat(response.rowCount()).isEqualTo(1L);
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
        assertBusy(() -> {
            execute("select count(*) from sys.nodes");
            assertThat(response).hasRows(Integer.toString(numNodes));
        });

        assertThatThrownBy(() -> execute("select * from my_schema.test"))
            .as("verify that the deleted index is removed from the cluster and not reimported as dangling by the restarted node")
            .hasMessageContaining("Schema 'my_schema' unknown");
        assertBusy(() -> {
            final NodeEnvironment nodeEnv = cluster().getInstance(NodeEnvironment.class);
            try {
                assertThat(nodeEnv.availableIndexFolders().contains(indexUUID)).as("index folder " + indexUUID + " should be deleted").isFalse();
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
        cluster().startNode();
        logger.info("--> indexing a simple document");
        execute("create table test (id int) with (number_of_replicas = 0)");
        execute("insert into test (id) values (1)");
        execute("refresh table test");
        logger.info("--> waiting for green status");
        assertBusy(() -> {
            execute("select health from sys.health where table_name = 'test'");
            assertThat(response).hasRows("GREEN");
        });
        ClusterState state = client()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();

        String indexUUID = resolveIndex("test").getUUID();
        final IndexMetadata metadata = state.metadata().index(indexUUID);
        final IndexMetadata.Builder brokenMeta = IndexMetadata.builder(metadata).settings(Settings.builder().put(metadata.getSettings())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion().internalId)
            // this is invalid but should be archived
            .put("index.similarity.BM25.type", "classic")
            // this one is not validated ahead of time and breaks allocation
            .put("index.analysis.filter.myCollator.type", "icu_collation"));
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).put(brokenMeta)));

        // check that the cluster does not keep reallocating shards
        assertBusy(() -> {
            final RoutingTable routingTable = client()
                .state(new ClusterStateRequest())
                .get()
                .getState().routingTable();
            final IndexRoutingTable indexRoutingTable = routingTable.index(indexUUID);
            assertThat(indexRoutingTable).isNotNull();
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                assertThat(shardRoutingTable.primaryShard().unassigned()).isTrue();
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().getLastAllocationStatus()).isEqualTo(UnassignedInfo.AllocationStatus.DECIDERS_NO);
                assertThat(shardRoutingTable.primaryShard().unassignedInfo().getNumFailedAllocations()).isGreaterThan(0);
            }
        }, 60, TimeUnit.SECONDS);
        execute("alter table test close");

        state = client()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();
        assertThat(state.metadata().index(metadata.getIndex()).getState()).isEqualTo(IndexMetadata.State.CLOSE);
        assertThat(state.metadata().index(metadata.getIndex()).getSettings().get("archived.index.similarity.BM25.type")).isEqualTo("classic");
        // try to open it with the broken setting - fail again!
        Asserts.assertSQLError(() -> execute("alter table test open"))
                .hasPGError(INTERNAL_ERROR)
                .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
                .hasMessageContaining("Failed to verify index " + metadata.getIndex().getUUID());
    }

    @Test
    public void testArchiveBrokenClusterSettings() throws Exception {
        logger.info("--> starting one node");
        cluster().startNode();
        execute("create table test (id int) with (number_of_replicas = 0)");
        execute("insert into test (id) values (1)");
        execute("refresh table test");
        logger.info("--> waiting for green status");
        assertBusy(() -> {
            assertThat(execute("select health from sys.health where table_name = 'test'")).hasRows("GREEN");
        });
        ClusterState state = client()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();

        final Metadata metadata = state.metadata();
        final Metadata brokenMeta = Metadata.builder(metadata).persistentSettings(Settings.builder()
                    .put(metadata.persistentSettings()).put("this.is.unknown", true)
                    .put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), "broken").build()).build();
        restartNodesOnBrokenClusterState(ClusterState.builder(state).metadata(brokenMeta));

        ensureYellow(); // wait for state recovery
        state = client()
            .state(new ClusterStateRequest())
            .get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS).getState();
        assertThat(state.metadata().persistentSettings().get("archived.this.is.unknown")).isEqualTo("true");
        assertThat(state.metadata().persistentSettings().get("archived."
            + SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey())).isEqualTo("broken");

        // delete these settings
        client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest()
                .persistentSettings(Settings.builder().putNull("archived.*"))
        ).get(REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        state = client().state(new ClusterStateRequest()).get().getState();
        assertThat(state.metadata().persistentSettings().get("archived.this.is.unknown")).isNull();
        assertThat(state.metadata().persistentSettings().get("archived."
            + SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey())).isNull();
        execute("select id from test");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    private void restartNodesOnBrokenClusterState(ClusterState.Builder clusterStateBuilder) throws Exception {
        Map<String, PersistedClusterStateService> lucenePersistedStateFactories = Stream.of(cluster().getNodeNames())
            .collect(Collectors.toMap(Function.identity(), nodeName -> cluster().getInstance(PersistedClusterStateService.class, nodeName)));
        final ClusterState clusterState = clusterStateBuilder.build();
        cluster().fullRestart(new RestartCallback() {
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
