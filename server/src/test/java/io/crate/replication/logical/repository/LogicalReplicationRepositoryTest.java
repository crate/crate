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
import static org.elasticsearch.test.ESTestCase.settings;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.junit.Test;

import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.LogicalReplicationSettings;

public class LogicalReplicationRepositoryTest {

    @Test
    public void test_getRemoteClusterState_upgrades_indexMetadata() throws Exception {
        // This test basically checks RelationMetadata was created based on IndexMetadata, where RelationMetadata
        // was introduced to 6.0, which implicitly verifies the upgrade.
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test")
                .settings(settings(Version.V_5_10_0))
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

        LogicalReplicationRepository logicalReplicationRepository = new LogicalReplicationRepository(
            mock(ClusterService.class),
            mock(LogicalReplicationService.class),
            new MetadataUpgradeService(createNodeContext(), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, null),
            remoteClusters,
            repositoryMetadata,
            mock(ThreadPool.class),
            mock(LogicalReplicationSettings.class)
        );

        ClusterStateResponse responseFromOldNode = logicalReplicationRepository.getRemoteClusterState(false, false, List.of()).get();
        assertThat(responseFromOldNode
            .getState().metadata().relations("doc", RelationMetadata.Table.class)
            .getFirst().name().toString()
        ).isEqualTo("doc.test");
    }
}
