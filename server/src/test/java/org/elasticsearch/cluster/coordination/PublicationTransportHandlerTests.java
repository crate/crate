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
package org.elasticsearch.cluster.coordination;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.cluster.coordination.PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

public class PublicationTransportHandlerTests extends ESTestCase {

    @Test
    public void testDiffSerializationFailure() {
        DeterministicTaskQueue deterministicTaskQueue =
            new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), random());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DiscoveryNode localNode = new DiscoveryNode("localNode", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportService transportService = new CapturingTransport().createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            x -> localNode,
            clusterSettings
        );
        final PublicationTransportHandler handler = new PublicationTransportHandler(transportService,
            writableRegistry(), null, pu -> null, (pu, l) -> {});
        transportService.start();
        transportService.acceptIncomingRequests();

        final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = CoordinationStateTests.clusterState(2L, 1L,
            DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build(),
            VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L);

        final ClusterState unserializableClusterState = new ClusterState(clusterState.version(),
            clusterState.stateUUID(), clusterState) {
            @Override
            public Diff<ClusterState> diff(Version version, ClusterState previousState) {
                return new Diff<ClusterState>() {
                    @Override
                    public ClusterState apply(ClusterState part) {
                        fail("this diff shouldn't be applied");
                        return part;
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        throw new IOException("Simulated failure of diff serialization");
                    }
                };
            }
        };

        assertThatThrownBy(() -> handler.newPublicationContext(new ClusterChangedEvent("test", unserializableClusterState, clusterState)))
            .isExactlyInstanceOf(ElasticsearchException.class)
            .cause()
                .isExactlyInstanceOf(IOException.class)
                .hasMessageContaining("Simulated failure of diff serialization");
    }

    /*
     * This test ensures BWC for templates and relations in a mixed cluster were nodes < 6.0.0 are using templates
     * for partitioned tables and nodes >= 6.0.0 are using relations, they do not read, write or process templates anymore.
     */
    @Test
    public void test_templates_received_from_5_are_upgraded_to_relations() throws Exception {
        DeterministicTaskQueue deterministicTaskQueue =
            new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), random());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiscoveryNode localNode = new DiscoveryNode("localNode", buildNewFakeTransportAddress(), Version.CURRENT);
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService localTransportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            x -> localNode,
            clusterSettings
        );

        AtomicReference<PublishRequest> publishRequestRef = new AtomicReference<>();

        MetadataUpgradeService metadataUpgradeService = new MetadataUpgradeService(createNodeContext(), null, null);
        new PublicationTransportHandler(
            localTransportService,
            writableRegistry(),
            metadataUpgradeService,
            pu -> {
                publishRequestRef.set(pu);
                return new PublishWithJoinResponse(new PublishResponse(pu.getAcceptedState().term(), pu.getAcceptedState().version()), Optional.empty());
            },
            (pu, l) -> {}
        );
        localTransportService.start();
        localTransportService.acceptIncomingRequests();

        RelationName relationName = new RelationName("my_schema", "my_table");
        RelationMetadata.Table table = new RelationMetadata.Table(
            relationName,
            List.of(new SimpleReference(
                ColumnIdent.of("x"),
                RowGranularity.PARTITION,
                DataTypes.STRING,
                IndexType.PLAIN,
                true,
                false,
                1,
                1,
                false,
                null
            )),
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .build(),
            null,
            ColumnPolicy.STRICT,
            null,
            Map.of(),
            List.of(),
            List.of(ColumnIdent.of("x")),
            IndexMetadata.State.OPEN,
            List.of(),
            0L
        );

        // Metadata will not write out any templates anymore as templates should not be used since 6.0.0.
        // But for nodes < 6.0.0, any partitioned table will be written out as a template, so this test is actually
        // testing both directions, writing out relations as templates and reading them back in as relations.
        // Thus, we will add a table here and not a template.
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder()
                .setTable(
                    table.name(),
                    table.columns(),
                    table.settings(),
                    table.routingColumn(),
                    table.columnPolicy(),
                    table.pkConstraintName(),
                    table.checkConstraints(),
                    table.primaryKeys(),
                    table.partitionedBy(),
                    table.state(),
                    table.indexUUIDs(),
                    table.tableVersion()
                )
                .build())
            .build();


        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
            stream.setVersion(Version.V_5_10_0);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        BytesReference serializedState = bStream.bytes();

        RequestHandlerRegistry<BytesTransportRequest> publishHandler = capturingTransport.getRequestHandlers().getHandler(PUBLISH_STATE_ACTION_NAME);
        publishHandler.processMessageReceived(new BytesTransportRequest(serializedState, Version.V_5_10_0), new TestTransportChannel(new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        }));

        ClusterState recievedClusterState = publishRequestRef.get().getAcceptedState();
        assertThat(recievedClusterState.metadata().templates().size()).isZero();

        RelationMetadata.Table table2 = recievedClusterState.metadata().getRelation(relationName);
        // We cannot check for equality of the table because the original table is not written by the DocTableInfo
        // and is such missing some (internal) fields.
        assertThat(table2).isNotNull();
        assertThat(table2.columns()).containsAll(table.columns());
        assertThat(table2.partitionedBy()).containsAll(table.partitionedBy());
    }

    @Test
    public void test_cluster_index_blocks_from_5_are_upgraded_to_index_uuids() throws Exception {
        DeterministicTaskQueue deterministicTaskQueue =
            new DeterministicTaskQueue(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), random());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiscoveryNode localNode = new DiscoveryNode("localNode", buildNewFakeTransportAddress(), Version.CURRENT);
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService localTransportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            x -> localNode,
            clusterSettings
        );

        AtomicReference<PublishRequest> publishRequestRef = new AtomicReference<>();

        MetadataUpgradeService metadataUpgradeService = new MetadataUpgradeService(createNodeContext(), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, null);
        new PublicationTransportHandler(
            localTransportService,
            writableRegistry(),
            metadataUpgradeService,
            pu -> {
                publishRequestRef.set(pu);
                return new PublishWithJoinResponse(new PublishResponse(pu.getAcceptedState().term(), pu.getAcceptedState().version()), Optional.empty());
            },
            (pu, l) -> {}
        );
        localTransportService.start();
        localTransportService.acceptIncomingRequests();

        RelationName relationName = new RelationName("my_schema", "my_table");

        String indexUUID = UUIDs.randomBase64UUID();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexUUID)
            .indexName(relationName.indexNameOrAlias())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                    .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_10_0)
                    .build()
                )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Metadata will not write out any templates anymore as templates should not be used since 6.0.0.
        // But for nodes < 6.0.0, any partitioned table will be written out as a template, so this test is actually
        // testing both directions, writing out relations as templates and reading them back in as relations.
        // Thus, we will add a table here and not a template.
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder()
                .put(indexMetadata, false)
                .build())
            .blocks(ClusterBlocks.builder().addIndexBlock(indexMetadata.getIndex().getName(), IndexMetadata.INDEX_READ_ONLY_BLOCK))
            .build();

        // Index block is registered by the index name.
        assertThat(clusterState.blocks().hasIndexBlock(relationName.indexNameOrAlias(), IndexMetadata.INDEX_READ_ONLY_BLOCK))
            .isTrue();
        assertThat(clusterState.blocks().hasIndexBlock(indexUUID, IndexMetadata.INDEX_READ_ONLY_BLOCK))
            .isFalse();

        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
            stream.setVersion(Version.V_5_10_0);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        BytesReference serializedState = bStream.bytes();

        RequestHandlerRegistry<BytesTransportRequest> publishHandler = capturingTransport.getRequestHandlers().getHandler(PUBLISH_STATE_ACTION_NAME);
        publishHandler.processMessageReceived(new BytesTransportRequest(serializedState, Version.V_5_10_0), new TestTransportChannel(new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        }));

        ClusterState recievedClusterState = publishRequestRef.get().getAcceptedState();

        // Index block is registered by the index UUID now.
        assertThat(recievedClusterState.blocks().hasIndexBlock(relationName.indexNameOrAlias(), IndexMetadata.INDEX_READ_ONLY_BLOCK))
            .isFalse();
        assertThat(recievedClusterState.blocks().hasIndexBlock(indexUUID, IndexMetadata.INDEX_READ_ONLY_BLOCK))
            .isTrue();
    }
}
