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

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsInProgressSerializationTests;
import org.elasticsearch.test.IntegTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.RandomShardRoutingMutator.randomChange;
import static org.elasticsearch.cluster.routing.RandomShardRoutingMutator.randomReason;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;


@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class ClusterStateDiffIT extends IntegTestCase {

    public void testClusterStateDiffSerialization() throws Exception {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        DiscoveryNode masterNode = randomNode("master");
        DiscoveryNode otherNode = randomNode("other");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(masterNode).add(otherNode).localNodeId(masterNode.getId()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        ClusterState clusterStateFromDiffs =
            ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState), otherNode, namedWriteableRegistry);

        int iterationCount = randomIntBetween(10, 300);
        for (int iteration = 0; iteration < iterationCount; iteration++) {
            ClusterState previousClusterState = clusterState;
            ClusterState previousClusterStateFromDiffs = clusterStateFromDiffs;
            int changesCount = randomIntBetween(1, 4);
            ClusterState.Builder builder = null;
            for (int i = 0; i < changesCount; i++) {
                if (i > 0) {
                    clusterState = builder.build();
                }
                switch (randomInt(5)) {
                    case 0:
                        builder = randomNodes(clusterState);
                        break;
                    case 1:
                        builder = randomRoutingTable(clusterState);
                        break;
                    case 2:
                        builder = randomBlocks(clusterState);
                        break;
                    case 3:
                        builder = randomClusterStateCustoms(clusterState);
                        break;
                    case 4:
                        builder = randomMetadataChanges(clusterState);
                        break;
                    case 5:
                        builder = randomCoordinationMetadata(clusterState);
                        break;
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
            }
            clusterState = builder.incrementVersion().build();

            if (randomIntBetween(0, 10) < 1) {
                // Update cluster state via full serialization from time to time
                clusterStateFromDiffs = ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState),
                    previousClusterStateFromDiffs.nodes().getLocalNode(), namedWriteableRegistry);
            } else {
                // Update cluster states using diffs
                Diff<ClusterState> diffBeforeSerialization = clusterState.diff(previousClusterState);
                BytesStreamOutput os = new BytesStreamOutput();
                diffBeforeSerialization.writeTo(os);
                byte[] diffBytes = BytesReference.toBytes(os.bytes());
                Diff<ClusterState> diff;
                try (StreamInput input = StreamInput.wrap(diffBytes)) {
                    StreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry);
                    diff = ClusterState.readDiffFrom(namedInput, previousClusterStateFromDiffs.nodes().getLocalNode());
                    clusterStateFromDiffs = diff.apply(previousClusterStateFromDiffs);
                }
            }


            try {
                // Check non-diffable elements
                assertThat(clusterStateFromDiffs.version()).isEqualTo(clusterState.version());
                assertThat(clusterStateFromDiffs.coordinationMetadata()).isEqualTo(clusterState.coordinationMetadata());

                // Check nodes
                assertThat(clusterStateFromDiffs.nodes().getNodes()).isEqualTo(clusterState.nodes().getNodes());
                assertThat(clusterStateFromDiffs.nodes().getLocalNodeId()).isEqualTo(previousClusterStateFromDiffs.nodes().getLocalNodeId());
                assertThat(clusterStateFromDiffs.nodes().getNodes()).isEqualTo(clusterState.nodes().getNodes());
                for (ObjectCursor<String> node : clusterStateFromDiffs.nodes().getNodes().keys()) {
                    DiscoveryNode node1 = clusterState.nodes().get(node.value);
                    DiscoveryNode node2 = clusterStateFromDiffs.nodes().get(node.value);
                    assertThat(node1.getVersion()).isEqualTo(node2.getVersion());
                    assertThat(node1.getAddress()).isEqualTo(node2.getAddress());
                    assertThat(node1.getAttributes()).isEqualTo(node2.getAttributes());
                }

                // Check routing table
                assertThat(clusterStateFromDiffs.routingTable().version()).isEqualTo(clusterState.routingTable().version());
                assertThat(clusterStateFromDiffs.routingTable().indicesRouting()).isEqualTo(clusterState.routingTable().indicesRouting());

                // Check cluster blocks
                assertThat(clusterStateFromDiffs.blocks().global()).isEqualTo(clusterStateFromDiffs.blocks().global());
                assertThat(clusterStateFromDiffs.blocks().indices()).isEqualTo(clusterStateFromDiffs.blocks().indices());
                assertThat(clusterStateFromDiffs.blocks().disableStatePersistence()).isEqualTo(clusterStateFromDiffs.blocks().disableStatePersistence());

                // Check metadata
                assertThat(clusterStateFromDiffs.metadata().version()).isEqualTo(clusterState.metadata().version());
                assertThat(clusterStateFromDiffs.metadata().clusterUUID()).isEqualTo(clusterState.metadata().clusterUUID());
                assertThat(clusterStateFromDiffs.metadata().transientSettings()).isEqualTo(clusterState.metadata().transientSettings());
                assertThat(clusterStateFromDiffs.metadata().persistentSettings()).isEqualTo(clusterState.metadata().persistentSettings());
                assertThat(clusterStateFromDiffs.metadata().indices()).isEqualTo(clusterState.metadata().indices());
                assertThat(clusterStateFromDiffs.metadata().templates()).isEqualTo(clusterState.metadata().templates());
                assertThat(clusterStateFromDiffs.metadata().customs()).isEqualTo(clusterState.metadata().customs());
                assertThat(clusterStateFromDiffs.metadata().equalsAliases(clusterState.metadata())).isTrue();

                // JSON Serialization test - make sure that both states produce similar JSON
                assertThat(differenceBetweenMapsIgnoringArrayOrder(convertToMap(clusterStateFromDiffs), convertToMap(clusterState))).isNull();

                // Smoke test - we cannot compare bytes to bytes because some elements might get serialized in different order
                // however, serialized size should remain the same
                assertThat(ClusterState.Builder.toBytes(clusterStateFromDiffs).length).isEqualTo(ClusterState.Builder.toBytes(clusterState).length);
            } catch (AssertionError error) {
                logger.error("Cluster state:\n{}\nCluster state from diffs:\n{}",
                    clusterState.toString(), clusterStateFromDiffs.toString());
                throw error;
            }
        }
    }

    private ClusterState.Builder randomCoordinationMetadata(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        CoordinationMetadata.Builder metaBuilder = CoordinationMetadata.builder(clusterState.coordinationMetadata());
        metaBuilder.term(randomNonNegativeLong());
        if (randomBoolean()) {
            metaBuilder.lastCommittedConfiguration(
                new CoordinationMetadata.VotingConfiguration(new HashSet<>(Arrays.asList(generateRandomStringArray(10, 10, false)))));
        }
        if (randomBoolean()) {
            metaBuilder.lastAcceptedConfiguration(
                new CoordinationMetadata.VotingConfiguration(new HashSet<>(Arrays.asList(generateRandomStringArray(10, 10, false)))));
        }
        if (randomBoolean()) {
            metaBuilder.addVotingConfigExclusion(new VotingConfigExclusion(randomNode("node-" + randomAlphaOfLength(10))));
        }
        return builder;
    }

    private DiscoveryNode randomNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), randomVersion(random()));
    }

    /**
     * Randomly updates nodes in the cluster state
     */
    private ClusterState.Builder randomNodes(ClusterState clusterState) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        List<String> nodeIds = randomSubsetOf(randomInt(clusterState.nodes().getNodes().size() - 1),
            clusterState.nodes().getNodes().keys().toArray(String.class));
        for (String nodeId : nodeIds) {
            if (nodeId.startsWith("node-")) {
                nodes.remove(nodeId);
                if (randomBoolean()) {
                    nodes.add(randomNode(nodeId));
                }
            }
        }
        int additionalNodeCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalNodeCount; i++) {
            nodes.add(randomNode("node-" + randomAlphaOfLength(10)));
        }
        return ClusterState.builder(clusterState).nodes(nodes);
    }

    /**
     * Randomly updates routing table in the cluster state
     */
    private ClusterState.Builder randomRoutingTable(ClusterState clusterState) {
        RoutingTable.Builder builder = RoutingTable.builder(clusterState.routingTable());
        int numberOfIndices = clusterState.routingTable().indicesRouting().size();
        if (numberOfIndices > 0) {
            List<String> randomIndices = randomSubsetOf(randomInt(numberOfIndices - 1),
                clusterState.routingTable().indicesRouting().keys().toArray(String.class));
            for (String index : randomIndices) {
                if (randomBoolean()) {
                    builder.remove(index);
                } else {
                    builder.add(randomChangeToIndexRoutingTable(clusterState.routingTable().indicesRouting().get(index),
                        clusterState.nodes().getNodes().keys().toArray(String.class)));
                }
            }
        }
        int additionalIndexCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalIndexCount; i++) {
            builder.add(randomIndexRoutingTable("index-" + randomInt(),
                clusterState.nodes().getNodes().keys().toArray(String.class)));
        }
        return ClusterState.builder(clusterState).routingTable(builder.build());
    }

    /**
     * Randomly updates index routing table in the cluster state
     */
    private IndexRoutingTable randomIndexRoutingTable(String index, String[] nodeIds) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(new Index(index, "_na_"));
        int shardCount = randomInt(10);

        for (int i = 0; i < shardCount; i++) {
            IndexShardRoutingTable.Builder indexShard = new IndexShardRoutingTable.Builder(new ShardId(index, "_na_", i));
            int replicaCount = randomIntBetween(1, 10);
            Set<String> availableNodeIds = new HashSet<>(Arrays.asList(nodeIds));
            for (int j = 0; j < replicaCount; j++) {
                UnassignedInfo unassignedInfo = null;
                if (randomInt(5) == 1) {
                    unassignedInfo = new UnassignedInfo(randomReason(), randomAlphaOfLength(10));
                }
                if (availableNodeIds.isEmpty()) {
                    break;
                }
                String nodeId = randomFrom(availableNodeIds);
                availableNodeIds.remove(nodeId);
                indexShard.addShard(
                        TestShardRouting.newShardRouting(index, i, nodeId, null, j == 0,
                                ShardRoutingState.fromValue((byte) randomIntBetween(2, 3)), unassignedInfo));
            }
            builder.addIndexShard(indexShard.build());
        }
        return builder.build();
    }

    /**
     * Randomly updates index routing table in the cluster state
     */
    private IndexRoutingTable randomChangeToIndexRoutingTable(IndexRoutingTable original, String[] nodes) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(original.getIndex());
        for (ObjectCursor<IndexShardRoutingTable> indexShardRoutingTable :  original.shards().values()) {
            Set<String> availableNodes = new HashSet<>(Arrays.asList(nodes));
            for (ShardRouting shardRouting : indexShardRoutingTable.value.shards()) {
                availableNodes.remove(shardRouting.currentNodeId());
                if (shardRouting.relocating()) {
                    availableNodes.remove(shardRouting.relocatingNodeId());
                }
            }

            for (ShardRouting shardRouting : indexShardRoutingTable.value.shards()) {
                final ShardRouting updatedShardRouting = randomChange(shardRouting, availableNodes);
                availableNodes.remove(updatedShardRouting.currentNodeId());
                if (shardRouting.relocating()) {
                    availableNodes.remove(updatedShardRouting.relocatingNodeId());
                }
                builder.addShard(updatedShardRouting);
            }
        }
        return builder.build();
    }

    /**
     * Randomly creates or removes cluster blocks
     */
    private ClusterState.Builder randomBlocks(ClusterState clusterState) {
        ClusterBlocks.Builder builder = ClusterBlocks.builder().blocks(clusterState.blocks());
        int globalBlocksCount = clusterState.blocks().global().size();
        if (globalBlocksCount > 0) {
            List<ClusterBlock> blocks = randomSubsetOf(randomInt(globalBlocksCount - 1),
                clusterState.blocks().global().toArray(new ClusterBlock[globalBlocksCount]));
            for (ClusterBlock block : blocks) {
                builder.removeGlobalBlock(block);
            }
        }
        int additionalGlobalBlocksCount = randomIntBetween(1, 3);
        for (int i = 0; i < additionalGlobalBlocksCount; i++) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        return ClusterState.builder(clusterState).blocks(builder);
    }

    /**
     * Returns a random global block
     */
    private ClusterBlock randomGlobalBlock() {
        switch (randomInt(2)) {
            case 0:
                return NoMasterBlockService.NO_MASTER_BLOCK_ALL;
            case 1:
                return NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
            default:
                return GatewayService.STATE_NOT_RECOVERED_BLOCK;
        }
    }

    /**
     * Random cluster state part generator interface. Used by {@link #randomClusterStateParts(ClusterState, String, RandomClusterPart)}
     * method to update cluster state with randomly generated parts
     */
    private interface RandomClusterPart<T> {
        /**
         * Returns list of parts from metadata
         */
        ImmutableOpenMap<String, T> parts(ClusterState clusterState);

        /**
         * Puts the part back into metadata
         */
        ClusterState.Builder put(ClusterState.Builder builder, T part);

        /**
         * Remove the part from metadata
         */
        ClusterState.Builder remove(ClusterState.Builder builder, String name);

        /**
         * Returns a random part with the specified name
         */
        T randomCreate(String name);

        /**
         * Makes random modifications to the part
         */
        T randomChange(T part);

    }

    /**
     * Takes an existing cluster state and randomly adds, removes or updates a cluster state part using randomPart generator.
     * If a new part is added the prefix value is used as a prefix of randomly generated part name.
     */
    private <T> ClusterState randomClusterStateParts(ClusterState clusterState, String prefix, RandomClusterPart<T> randomPart) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        ImmutableOpenMap<String, T> parts = randomPart.parts(clusterState);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(randomInt(partCount - 1),
                randomPart.parts(clusterState).keys().toArray(String.class));
            for (String part : randomParts) {
                if (randomBoolean()) {
                    randomPart.remove(builder, part);
                } else {
                    randomPart.put(builder, randomPart.randomChange(parts.get(part)));
                }
            }
        }
        int additionalPartCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalPartCount; i++) {
            String name = randomName(prefix);
            randomPart.put(builder, randomPart.randomCreate(name));
        }
        return builder.build();
    }

    /**
     * Makes random metadata changes
     */
    private ClusterState.Builder randomMetadataChanges(ClusterState clusterState) {
        Metadata metadata = clusterState.metadata();
        int changesCount = randomIntBetween(1, 10);
        for (int i = 0; i < changesCount; i++) {
            switch (randomInt(3)) {
                case 0:
                    metadata = randomMetadataSettings(metadata);
                    break;
                case 1:
                    metadata = randomIndices(metadata);
                    break;
                case 2:
                    metadata = randomTemplates(metadata);
                    break;
                case 3:
                    metadata = randomMetadataCustoms(metadata);
                    break;
                default:
                    throw new IllegalArgumentException("Shouldn't be here");
            }
        }
        return ClusterState.builder(clusterState).metadata(Metadata.builder(metadata).version(metadata.version() + 1).build());
    }

    /**
     * Makes random settings changes
     */
    private Settings randomSettings(Settings settings) {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(settings);
        }
        int settingsCount = randomInt(10);
        for (int i = 0; i < settingsCount; i++) {
            builder.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return builder.build();

    }

    /**
     * Randomly updates persistent or transient settings of the given metadata
     */
    private Metadata randomMetadataSettings(Metadata metadata) {
        if (randomBoolean()) {
            return Metadata.builder(metadata).persistentSettings(randomSettings(metadata.persistentSettings())).build();
        } else {
            return Metadata.builder(metadata).transientSettings(randomSettings(metadata.transientSettings())).build();
        }
    }

    /**
     * Random metadata part generator
     */
    private interface RandomPart<T> {
        /**
         * Returns list of parts from metadata
         */
        ImmutableOpenMap<String, T> parts(Metadata metadata);

        /**
         * Puts the part back into metadata
         */
        Metadata.Builder put(Metadata.Builder builder, T part);

        /**
         * Remove the part from metadata
         */
        Metadata.Builder remove(Metadata.Builder builder, String name);

        /**
         * Returns a random part with the specified name
         */
        T randomCreate(String name);

        /**
         * Makes random modifications to the part
         */
        T randomChange(T part);

    }

    /**
     * Takes an existing cluster state and randomly adds, removes or updates a metadata part using randomPart generator.
     * If a new part is added the prefix value is used as a prefix of randomly generated part name.
     */
    private <T> Metadata randomParts(Metadata metadata, String prefix, RandomPart<T> randomPart) {
        Metadata.Builder builder = Metadata.builder(metadata);
        ImmutableOpenMap<String, T> parts = randomPart.parts(metadata);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(randomInt(partCount - 1),
                randomPart.parts(metadata).keys().toArray(String.class));
            for (String part : randomParts) {
                if (randomBoolean()) {
                    randomPart.remove(builder, part);
                } else {
                    randomPart.put(builder, randomPart.randomChange(parts.get(part)));
                }
            }
        }
        int additionalPartCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalPartCount; i++) {
            String name = randomName(prefix);
            randomPart.put(builder, randomPart.randomCreate(name));
        }
        return builder.build();
    }

    /**
     * Randomly add, deletes or updates indices in the metadata
     */
    private Metadata randomIndices(Metadata metadata) {
        return randomParts(metadata, "index", new RandomPart<IndexMetadata>() {

            @Override
            public ImmutableOpenMap<String, IndexMetadata> parts(Metadata metadata) {
                return metadata.indices();
            }

            @Override
            public Metadata.Builder put(Metadata.Builder builder, IndexMetadata part) {
                return builder.put(part, true);
            }

            @Override
            public Metadata.Builder remove(Metadata.Builder builder, String name) {
                return builder.remove(name);
            }

            @Override
            public IndexMetadata randomCreate(String name) {
                IndexMetadata.Builder builder = IndexMetadata.builder(name);
                Settings.Builder settingsBuilder = Settings.builder();
                setRandomIndexSettings(random(), settingsBuilder);
                settingsBuilder.put(randomSettings(Settings.EMPTY)).put(IndexMetadata.SETTING_VERSION_CREATED, randomVersion(random()));
                builder.settings(settingsBuilder);
                builder.numberOfShards(randomIntBetween(1, 10)).numberOfReplicas(randomInt(10));
                int aliasCount = randomInt(10);
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                return builder.build();
            }

            @Override
            public IndexMetadata randomChange(IndexMetadata part) {
                IndexMetadata.Builder builder = IndexMetadata.builder(part);
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        builder.settings(Settings.builder().put(part.getSettings()).put(randomSettings(Settings.EMPTY)));
                        break;
                    case 1:
                        if (randomBoolean() && part.getAliases().isEmpty() == false) {
                            builder.removeAlias(randomFrom(part.getAliases().keys().toArray(String.class)));
                        } else {
                            builder.putAlias(AliasMetadata.builder(randomAlphaOfLength(10)));
                        }
                        break;
                    case 2:
                        builder.settings(Settings.builder().put(part.getSettings())
                            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()));
                        break;
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
                return builder.build();
            }
        });
    }

    /**
     * Randomly adds, deletes or updates index templates in the metadata
     */
    private Metadata randomTemplates(Metadata metadata) {
        return randomParts(metadata, "template", new RandomPart<IndexTemplateMetadata>() {
            @Override
            public ImmutableOpenMap<String, IndexTemplateMetadata> parts(Metadata metadata) {
                return metadata.templates();
            }

            @Override
            public Metadata.Builder put(Metadata.Builder builder, IndexTemplateMetadata part) {
                return builder.put(part);
            }

            @Override
            public Metadata.Builder remove(Metadata.Builder builder, String name) {
                return builder.removeTemplate(name);
            }

            @Override
            public IndexTemplateMetadata randomCreate(String name) {
                IndexTemplateMetadata.Builder builder = IndexTemplateMetadata.builder(name);
                try {
                    builder.order(randomInt(1000))
                            .patterns(Collections.singletonList(randomName("temp")))
                            .settings(randomSettings(Settings.EMPTY))
                            .putMapping("{}");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                int aliasCount = randomIntBetween(0, 10);
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                return builder.build();
            }

            @Override
            public IndexTemplateMetadata randomChange(IndexTemplateMetadata part) {
                IndexTemplateMetadata.Builder builder = new IndexTemplateMetadata.Builder(part);
                builder.order(randomInt(1000));
                return builder.build();
            }
        });
    }

    /**
     * Generates random alias
     */
    private AliasMetadata randomAlias() {
        AliasMetadata.Builder builder = new AliasMetadata.Builder(randomName("alias"));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLength(10));
        }
        return builder.build();
    }

    /**
     * Randomly adds, deletes or updates repositories in the metadata
     */
    private Metadata randomMetadataCustoms(final Metadata metadata) {
        return randomParts(metadata, "custom", new RandomPart<Metadata.Custom>() {

            @Override
            public ImmutableOpenMap<String, Metadata.Custom> parts(Metadata metadata) {
                return metadata.customs();
            }

            @Override
            public Metadata.Builder put(Metadata.Builder builder, Metadata.Custom part) {
                return builder.putCustom(part.getWriteableName(), part);
            }

            @Override
            public Metadata.Builder remove(Metadata.Builder builder, String name) {
                if (IndexGraveyard.TYPE.equals(name)) {
                    // there must always be at least an empty graveyard
                    return builder.indexGraveyard(IndexGraveyard.builder().build());
                } else {
                    return builder.removeCustom(name);
                }
            }

            @Override
            public Metadata.Custom randomCreate(String name) {
                if (randomBoolean()) {
                    return new RepositoriesMetadata(Collections.emptyList());
                } else {
                    final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
                    final int numTombstones = randomIntBetween(0, 4);
                    for (int i = 0; i < numTombstones; i++) {
                        graveyard.addTombstone(new Index("idx-" + i, UUIDs.randomBase64UUID()));
                    }
                    return graveyard.build();
                }
            }

            @Override
            public Metadata.Custom randomChange(Metadata.Custom part) {
                return part;
            }
        });
    }

    /**
     * Randomly adds, deletes or updates in-progress snapshot and restore records in the cluster state
     */
    private ClusterState.Builder randomClusterStateCustoms(final ClusterState clusterState) {
        return ClusterState.builder(randomClusterStateParts(clusterState, "custom", new RandomClusterPart<ClusterState.Custom>() {

            @Override
            public ImmutableOpenMap<String, ClusterState.Custom> parts(ClusterState clusterState) {
                return clusterState.customs();
            }

            @Override
            public ClusterState.Builder put(ClusterState.Builder builder, ClusterState.Custom part) {
                return builder.putCustom(part.getWriteableName(), part);
            }

            @Override
            public ClusterState.Builder remove(ClusterState.Builder builder, String name) {
                return builder.removeCustom(name);
            }

            @Override
            public ClusterState.Custom randomCreate(String name) {
                switch (randomIntBetween(0, 1)) {
                    case 0:
                        return SnapshotsInProgress.of(Collections.singletonList(new SnapshotsInProgress.Entry(
                                new Snapshot(randomName("repo"), new SnapshotId(randomName("snap"), UUIDs.randomBase64UUID())),
                                randomBoolean(),
                                randomBoolean(),
                                SnapshotsInProgressSerializationTests.randomState(ImmutableOpenMap.of()),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Math.abs(randomLong()),
                                randomIntBetween(0, 1000),
                                ImmutableOpenMap.of(),
                                null,
                                randomVersion(random()))));
                    case 1:
                        return new RestoreInProgress.Builder().add(
                            new RestoreInProgress.Entry(
                                UUIDs.randomBase64UUID(),
                                new Snapshot(randomName("repo"), new SnapshotId(randomName("snap"), UUIDs.randomBase64UUID())),
                                RestoreInProgress.State.fromValue((byte) randomIntBetween(0, 3)),
                                emptyList(),
                                ImmutableOpenMap.of())).build();
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
            }

            @Override
            public ClusterState.Custom randomChange(ClusterState.Custom part)  {
                return part;
            }
        }));
    }

    /**
     * Generates a random name that starts with the given prefix
     */
    private String randomName(String prefix) {
        return prefix + UUIDs.randomBase64UUID(random());
    }
}
