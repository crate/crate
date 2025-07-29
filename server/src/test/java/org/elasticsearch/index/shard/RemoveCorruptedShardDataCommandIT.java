/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.index.shard;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import io.crate.server.cli.MockTerminal;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoveCorruptedShardDataCommandIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    public void testCorruptIndex() throws Exception {
        final String node = internalCluster().startNode();

        final String indexName = "index42";
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")
        ));

        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
            }

            numDocs += numExtraDocs;

            indexRandom(false, false, false, Arrays.asList(builders));
            flush(indexName);
        }

        logger.info("--> indexed {} docs", numDocs);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal terminal = new MockTerminal();
        final OptionParser parser = command.getParser();

        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        final OptionSet options = parser.parse("-index", indexName, "-shard-id", "0");

        // Try running it before the node is stopped (and shard is closed)
        try {
            command.execute(terminal, options, environment);
            fail("expected the command to fail as node is locked");
        } catch (Exception e) {
            assertThat(e.getMessage(),
                allOf(containsString("Failed to lock node's directory"),
                    containsString("is Elasticsearch still running ?")));
        }

        final Set<Path> indexDirs = getDirs(indexName, ShardPath.INDEX_FOLDER_NAME);
        assertThat(indexDirs, hasSize(1));

        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // Try running it before the shard is corrupted, it should flip out because there is no corruption file marker
                try {
                    command.execute(terminal, options, environment);
                    fail("expected the command to fail as there is no corruption file marker");
                } catch (Exception e) {
                    assertThat(e.getMessage(), startsWith("Shard does not seem to be corrupted at"));
                }

                CorruptionUtils.corruptIndex(random(), indexDirs.iterator().next(), false);
                return super.onNodeStopped(nodeName);
            }
        });

        // shard should be failed due to a corrupted index
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(0).setPrimary(true)
                    .get().getExplanation();

            final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY));
        });

        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                terminal.addTextInput("y");
                command.execute(terminal, options, environment);

                return super.onNodeStopped(nodeName);
            }
        });

        waitNoPendingTasksOnAll();

        String nodeId = null;
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getNodes()) {
            final String name = cursor.value.getName();
            if (name.equals(node)) {
                nodeId = cursor.key;
                break;
            }
        }
        assertThat(nodeId, notNullValue());

        logger.info("--> output:\n{}", terminal.getOutput());

        assertThat(terminal.getOutput(), containsString("allocate_stale_primary"));
        assertThat(terminal.getOutput(), containsString("\"node\" : \"" + nodeId + "\""));

        // there is only _stale_ primary (due to new allocation id)
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(0).setPrimary(true)
                    .get().getExplanation();

            final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY));
        });

        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, nodeId, true))
            .get();

        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(0).setPrimary(true)
                    .get().getExplanation();

            assertThat(explanation.getCurrentNode(), notNullValue());
            assertThat(explanation.getShardState(), equalTo(ShardRoutingState.STARTED));
        });

        final Pattern pattern = Pattern.compile("Corrupted Lucene index segments found -\\s+(?<docs>\\d+) documents will be lost.");
        final Matcher matcher = pattern.matcher(terminal.getOutput());
        assertThat(matcher.find(), equalTo(true));
        final int expectedNumDocs = numDocs - Integer.parseInt(matcher.group("docs"));

        ensureGreen(indexName);

        assertHitCount(client().prepareSearch(indexName).setQuery(matchAllQuery()).get(), expectedNumDocs);
    }

    public void testCorruptTranslogTruncation() throws Exception {
        internalCluster().startNodes(2, Settings.EMPTY);

        final String node1 = internalCluster().getNodeNames()[0];
        final String node2 = internalCluster().getNodeNames()[1];

        final String indexName = "test";
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
            .put("index.routing.allocation.exclude._name", node2)
        ));
        ensureYellow();

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", (String)null)
        ));
        ensureGreen();

        // Index some documents
        int numDocsToKeep = randomIntBetween(10, 100);
        logger.info("--> indexing [{}] docs to be kept", numDocsToKeep);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocsToKeep];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        flush(indexName);

        disableTranslogFlush(indexName);
        // having no extra docs is an interesting case for seq no based recoveries - test it more often
        int numDocsToTruncate = randomBoolean() ? 0 : randomIntBetween(0, 100);
        logger.info("--> indexing [{}] more doc to be truncated", numDocsToTruncate);
        builders = new IndexRequestBuilder[numDocsToTruncate];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        Set<Path> translogDirs = getDirs(indexName, ShardPath.TRANSLOG_FOLDER_NAME);

        // that's only for 6.x branch for bwc with elasticsearch-translog
        final boolean translogOnly = randomBoolean();
        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand(translogOnly);
        final MockTerminal terminal = new MockTerminal();
        final OptionParser parser = command.getParser();

        if (randomBoolean() && numDocsToTruncate > 0) {
            // flush the replica, so it will have more docs than what the primary will have
            Index index = resolveIndex(indexName);
            IndexShard replica = internalCluster().getInstance(IndicesService.class, node2).getShardOrNull(new ShardId(index, 0));
            replica.flush(new FlushRequest());
            logger.info("--> performed extra flushing on replica");
        }

        // shut down the replica node to be tested later
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node2));

        // Corrupt the translog file(s)
        logger.info("--> corrupting translog");
        corruptRandomTranslogFiles(indexName);

        // Restart the single node
        logger.info("--> restarting node");
        internalCluster().restartRandomDataNode();

        // all shards should be failed due to a corrupted translog
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(0).setPrimary(true)
                    .get().getExplanation();

            final UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
            assertThat(unassignedInfo.getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        });

        // have to shut down primary node - otherwise node lock is present
        final InternalTestCluster.RestartCallback callback =
            new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    // and we can actually truncate the translog
                    for (Path translogDir : translogDirs) {
                        final Path idxLocation = translogDir.getParent().resolve(ShardPath.INDEX_FOLDER_NAME);
                        assertBusy(() -> {
                            logger.info("--> checking that lock has been released for {}", idxLocation);
                            try (Directory dir = FSDirectory.open(idxLocation, NativeFSLockFactory.INSTANCE);
                                 Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                                // Great, do nothing, we just wanted to obtain the lock
                            } catch (LockObtainFailedException lofe) {
                                logger.info("--> failed acquiring lock for {}", idxLocation);
                                fail("still waiting for lock release at [" + idxLocation + "]");
                            } catch (IOException ioe) {
                                fail("Got an IOException: " + ioe);
                            }
                        });

                        final Settings defaultSettings = internalCluster().getDefaultSettings();
                        final Environment environment = TestEnvironment.newEnvironment(defaultSettings);

                        terminal.addTextInput("y");
                        OptionSet options = parser.parse("-d", translogDir.toAbsolutePath().toString());
                        logger.info("--> running command for [{}]", translogDir.toAbsolutePath());
                        command.execute(terminal, options, environment);
                        logger.info("--> output:\n{}", terminal.getOutput());
                    }

                    return super.onNodeStopped(nodeName);
                }
            };
        internalCluster().restartNode(node1, callback);

        String primaryNodeId = null;
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getNodes()) {
            final String name = cursor.value.getName();
            if (name.equals(node1)) {
                primaryNodeId = cursor.key;
                break;
            }
        }
        assertThat(primaryNodeId, notNullValue());

        assertThat(terminal.getOutput(), containsString("allocate_stale_primary"));
        assertThat(terminal.getOutput(), containsString("\"node\" : \"" + primaryNodeId + "\""));

        // there is only _stale_ primary (due to new allocation id)
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(0).setPrimary(true)
                    .get().getExplanation();

            final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY));
        });

        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId, true))
            .get();

        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(0).setPrimary(true)
                    .get().getExplanation();

            assertThat(explanation.getCurrentNode(), notNullValue());
            assertThat(explanation.getShardState(), equalTo(ShardRoutingState.STARTED));
        });

        ensureYellow(indexName);

        // Run a search and make sure it succeeds
        assertHitCount(client().prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocsToKeep);

        logger.info("--> starting the replica node to test recovery");
        internalCluster().startNode();
        ensureGreen(indexName);
        for (String node : internalCluster().nodesInclude(indexName)) {
            SearchRequestBuilder q = client().prepareSearch(indexName).setPreference("_only_nodes:" + node).setQuery(matchAllQuery());
            assertHitCount(q.get(), numDocsToKeep);
        }
        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).setActiveOnly(false).get();
        final RecoveryState replicaRecoveryState = recoveryResponse.shardRecoveryStates().get(indexName).stream()
            .filter(recoveryState -> recoveryState.getPrimary() == false).findFirst().get();
        assertThat(replicaRecoveryState.getIndex().toString(), replicaRecoveryState.getIndex().recoveredFileCount(), greaterThan(0));
        // Ensure that the global checkpoint and local checkpoint are restored from the max seqno of the last commit.
        final SeqNoStats seqNoStats = getSeqNoStats(indexName, 0);
        assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
    }

    public void testCorruptTranslogTruncationOfReplica() throws Exception {
        internalCluster().startNodes(2, Settings.EMPTY);

        final String node1 = internalCluster().getNodeNames()[0];
        final String node2 = internalCluster().getNodeNames()[1];
        logger.info("--> nodes name: {}, {}", node1, node2);

        final String indexName = "test";
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
            .put("index.routing.allocation.exclude._name", node2)
        ));
        ensureYellow();

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", (String)null)
        ));
        ensureGreen();

        // Index some documents
        int numDocsToKeep = randomIntBetween(0, 100);
        logger.info("--> indexing [{}] docs to be kept", numDocsToKeep);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocsToKeep];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        flush(indexName);
        disableTranslogFlush(indexName);
        // having no extra docs is an interesting case for seq no based recoveries - test it more often
        int numDocsToTruncate = randomBoolean() ? 0 : randomIntBetween(0, 100);
        logger.info("--> indexing [{}] more docs to be truncated", numDocsToTruncate);
        builders = new IndexRequestBuilder[numDocsToTruncate];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        final int totalDocs = numDocsToKeep + numDocsToTruncate;

        // sample the replica node translog dirs
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Set<Path> translogDirs = getDirs(node2, shardId, ShardPath.TRANSLOG_FOLDER_NAME);

        // stop the cluster nodes. we don't use full restart so the node start up order will be the same
        // and shard roles will be maintained
        internalCluster().stopRandomDataNode();
        internalCluster().stopRandomDataNode();

        // Corrupt the translog file(s)
        logger.info("--> corrupting translog");
        TestTranslog.corruptRandomTranslogFile(logger, random(), translogDirs);

        // Restart the single node
        logger.info("--> starting node");
        internalCluster().startNode();

        ensureYellow();

        // Run a search and make sure it succeeds
        assertHitCount(client().prepareSearch(indexName).setQuery(matchAllQuery()).get(), totalDocs);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal terminal = new MockTerminal();
        final OptionParser parser = command.getParser();

        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());

        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                logger.info("--> node {} stopped", nodeName);
                for (Path translogDir : translogDirs) {
                    final Path idxLocation = translogDir.getParent().resolve(ShardPath.INDEX_FOLDER_NAME);
                    assertBusy(() -> {
                        logger.info("--> checking that lock has been released for {}", idxLocation);
                        try (Directory dir = FSDirectory.open(idxLocation, NativeFSLockFactory.INSTANCE);
                             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                            // Great, do nothing, we just wanted to obtain the lock
                        }  catch (LockObtainFailedException lofe) {
                            logger.info("--> failed acquiring lock for {}", idxLocation);
                            fail("still waiting for lock release at [" + idxLocation + "]");
                        } catch (IOException ioe) {
                            fail("Got an IOException: " + ioe);
                        }
                    });

                    terminal.addTextInput("y");
                    OptionSet options = parser.parse("-d", translogDir.toAbsolutePath().toString());
                    logger.info("--> running command for [{}]", translogDir.toAbsolutePath());
                    command.execute(terminal, options, environment);
                    logger.info("--> output:\n{}", terminal.getOutput());
                }

                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> starting the replica node to test recovery");
        internalCluster().startNode();
        ensureGreen(indexName);
        for (String node : internalCluster().nodesInclude(indexName)) {
            assertHitCount(client().prepareSearch(indexName)
                .setPreference("_only_nodes:" + node).setQuery(matchAllQuery()).get(), totalDocs);
        }

        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).setActiveOnly(false).get();
        final RecoveryState replicaRecoveryState = recoveryResponse.shardRecoveryStates().get(indexName).stream()
            .filter(recoveryState -> recoveryState.getPrimary() == false).findFirst().get();
        // the replica translog was disabled so it doesn't know what hte global checkpoint is and thus can't do ops based recovery
        assertThat(replicaRecoveryState.getIndex().toString(), replicaRecoveryState.getIndex().recoveredFileCount(), greaterThan(0));
        // Ensure that the global checkpoint and local checkpoint are restored from the max seqno of the last commit.
        final SeqNoStats seqNoStats = getSeqNoStats(indexName, 0);
        assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
    }

    public void testResolvePath() throws Exception {
        final int numOfNodes = randomIntBetween(1, 5);
        final List<String> nodeNames = internalCluster().startNodes(numOfNodes, Settings.EMPTY);

        final String indexName = "test" + randomInt(100);
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfNodes - 1)
        ));
        flush(indexName);

        ensureGreen(indexName);

        final Map<String, String> nodeNameToNodeId = new HashMap<>();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getNodes()) {
            nodeNameToNodeId.put(cursor.value.getName(), cursor.key);
        }

        final GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[]{indexName}, false);
        final List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        final ShardRouting shardRouting = iterators.iterator().next().nextOrNull();
        assertThat(shardRouting, notNullValue());
        final ShardId shardId = shardRouting.shardId();

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final OptionParser parser = command.getParser();

        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());

        final Map<String, Path> indexPathByNodeName = new HashMap<>();
        for (String nodeName : nodeNames) {
            final String nodeId = nodeNameToNodeId.get(nodeName);
            final Set<Path> indexDirs = getDirs(nodeId, shardId, ShardPath.INDEX_FOLDER_NAME);
            assertThat(indexDirs, hasSize(1));
            indexPathByNodeName.put(nodeName, indexDirs.iterator().next());

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeName));
            logger.info(" -- stopped {}", nodeName);
        }

        for (String nodeName : nodeNames) {
            final Path indexPath = indexPathByNodeName.get(nodeName);
            final OptionSet options = parser.parse("--dir", indexPath.toAbsolutePath().toString());
            command.findAndProcessShardPath(options, environment,
                shardPath -> assertThat(shardPath.resolveIndex(), equalTo(indexPath)));
        }
    }

    private Set<Path> getDirs(String indexName, String dirSuffix) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[]{indexName}, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        ShardId shardId = shardRouting.shardId();
        return getDirs(nodeId, shardId, dirSuffix);
    }

    private Set<Path> getDirs(String nodeId, ShardId shardId, String dirSuffix) {
        final NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        final Set<Path> translogDirs = new TreeSet<>();
        final NodeStats nodeStats = nodeStatses.getNodes().get(0);
        for (FsInfo.Path fsPath : nodeStats.getFs()) {
            final String path = fsPath.getPath();
            final Path p = PathUtils.get(path)
                .resolve(NodeEnvironment.INDICES_FOLDER)
                .resolve(shardId.getIndex().getUUID())
                .resolve(Integer.toString(shardId.getId()))
                .resolve(dirSuffix);
            if (Files.isDirectory(p)) {
                translogDirs.add(p);
            }
        }
        return translogDirs;
    }

    private void corruptRandomTranslogFiles(String indexName) throws IOException {
        Set<Path> translogDirs = getDirs(indexName, ShardPath.TRANSLOG_FOLDER_NAME);
        TestTranslog.corruptRandomTranslogFile(logger, random(), translogDirs);
    }

    /** Disables translog flushing for the specified index */
    private static void disableTranslogFlush(String index) {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
            .build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    private SeqNoStats getSeqNoStats(String index, int shardId) {
        final ShardStats[] shardStats = client().admin().indices()
            .prepareStats(index).get()
            .getIndices().get(index).getShards();
        return shardStats[shardId].getSeqNoStats();
    }
}
