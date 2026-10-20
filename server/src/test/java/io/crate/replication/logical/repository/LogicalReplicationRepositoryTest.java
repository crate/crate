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

package io.crate.replication.logical.repository;

import static io.crate.replication.logical.repository.LogicalReplicationRepository.REMOTE_REPOSITORY_PREFIX;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.junit.Test;

import io.crate.concurrent.FutureActionListener;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.replication.logical.action.PublicationsStateAction;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.Subscription;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationMetadata;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.LogicalReplicationSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class LogicalReplicationRepositoryTest extends CrateDummyClusterServiceUnitTest {

    @Test
    @SuppressWarnings("deprecation")
    public void test_getRemoteClusterState_upgrades_indexMetadata() throws Exception {
        // This test basically checks RelationMetadata was created based on IndexMetadata, where RelationMetadata
        // was introduced to 6.0, which implicitly verifies the upgrade.
        Metadata metadata = new Metadata.Builder(Metadata.OID_UNASSIGNED)
            .put(IndexMetadata.builder("test")
                .settings(settings(Version.V_5_10_0))
                .putMapping("{}")
                .numberOfShards(1)
                .numberOfReplicas(1))
            .build();
        ClusterState clusterState = new ClusterState.Builder(
            ClusterName.DEFAULT).metadata(metadata).build();

        ClusterStateResponse resp = new ClusterStateResponse(
            ClusterName.DEFAULT,
            clusterState,
            false
        );

        RemoteClusters remoteClusters = mock(RemoteClusters.class);
        Client remoteClient = mock(Client.class);
        RepositoryMetadata repositoryMetadata = mock(RepositoryMetadata.class);

        when(remoteClusters.getClient(any())).thenReturn(remoteClient);
        when(remoteClient.state(any())).thenReturn(CompletableFuture.completedFuture(resp));
        when(repositoryMetadata.name()).thenReturn(REMOTE_REPOSITORY_PREFIX);

        NodeContext nodeContext = createNodeContext();
        LogicalReplicationRepository logicalReplicationRepository = new LogicalReplicationRepository(
            mock(ClusterService.class),
            mock(LogicalReplicationService.class),
            new MetadataUpgradeService(
                nodeContext,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                new UserDefinedFunctionService(clusterService, nodeContext)
            ),
            remoteClusters,
            repositoryMetadata,
            mock(ThreadPool.class),
            mock(LogicalReplicationSettings.class)
        );

        ClusterStateResponse responseFromOldNode = logicalReplicationRepository.getRemoteClusterState(false, false, List.of()).get();
        logicalReplicationRepository.close();
        assertThat(responseFromOldNode
            .getState().metadata().relations("doc", RelationMetadata.Table.class)
            .getFirst().name().toString()
        ).isEqualTo("doc.test");
    }

    @Test
    public void test_deleted_publisher_shard_completes_recovery_listener_with_failure() throws Exception {
        Client remoteClient = mock(Client.class);
        RemoteClusters remoteClusters = mock(RemoteClusters.class);
        when(remoteClusters.getClient(any())).thenReturn(remoteClient);
        when(remoteClient.execute(any(PublicationsStateAction.class), any()))
            .thenReturn(CompletableFuture.completedFuture(new PublicationsStateAction.Response(Metadata.EMPTY_METADATA, List.of())));
        when(remoteClient.state(any())).thenReturn(CompletableFuture.completedFuture(
            new ClusterStateResponse(ClusterName.DEFAULT, ClusterState.builder(ClusterName.DEFAULT).build(), false)));
        LogicalReplicationService service = mock(LogicalReplicationService.class);
        when(service.subscriptions()).thenReturn(Map.of("sub", new Subscription(
            "crate", ConnectionInfo.fromURL("crate://localhost?user=crate"), List.of("pub"), Settings.EMPTY, Map.of())));
        RepositoryMetadata repositoryMetadata = mock(RepositoryMetadata.class);
        when(repositoryMetadata.name()).thenReturn(REMOTE_REPOSITORY_PREFIX + "sub");
        var nodeContext = createNodeContext();
        try (var repository = new LogicalReplicationRepository(
                clusterService, service,
                new MetadataUpgradeService(nodeContext, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                    new UserDefinedFunctionService(clusterService, nodeContext)),
                remoteClusters, repositoryMetadata, mock(ThreadPool.class), mock(LogicalReplicationSettings.class))) {
            var completion = new FutureActionListener<Void>();
            repository.restoreShardUsingMultiChunkTransfer(mock(Store.class), new IndexId("test", "gone"),
                new ShardId("test", "gone", 0), mock(RecoveryState.class), completion);
            assertThatThrownBy(() -> completion.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IndexNotFoundException.class);
        }
    }

}
