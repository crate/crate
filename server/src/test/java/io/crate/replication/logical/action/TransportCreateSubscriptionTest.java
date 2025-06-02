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

package io.crate.replication.logical.action;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.concurrent.FutureActionListener;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.role.Roles;
import io.crate.role.StubRoleManager;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.QualifiedName;

public class TransportCreateSubscriptionTest {

    private final LogicalReplicationService logicalReplicationService = mock(LogicalReplicationService.class);
    private final Roles roles = new StubRoleManager();
    private final ClusterService clusterService = mock(ClusterService.class);

    @Test
    public void test_subscribing_to_tables_with_higher_version_raises_error() throws Exception {
        FutureActionListener<AcknowledgedResponse> responseFuture = new FutureActionListener<>();
        subscribeToTablesWithVersion(responseFuture, Version.CURRENT.internalId + 10000, true);

        var tableVersion = Version.fromId(Version.CURRENT.internalId + 10000);
        assertThatThrownBy(responseFuture::get)
            .hasMessageContaining("One of the published tables has version higher than subscriber's minimal node version." +
                " Table=doc.t1, Table-Version=" + tableVersion + ", Local-Minimal-Version: " + Version.CURRENT);

        // Repeat, but with an unsupported version inside the index of the relation
        responseFuture = new FutureActionListener<>();
        subscribeToTablesWithVersion(responseFuture, Version.CURRENT.internalId + 10000, false);
        assertThatThrownBy(responseFuture::get)
            .hasMessageContaining("One of the published tables has version higher than subscriber's minimal node version." +
                " Table=doc.t1, Table-Version=" + tableVersion + ", Local-Minimal-Version: " + Version.CURRENT);
    }

    @Test
    public void test_subscribing_to_tables_with_higher_hotfix_works() throws Exception {
        FutureActionListener<AcknowledgedResponse> responseFuture = new FutureActionListener<>();
        subscribeToTablesWithVersion(responseFuture, Version.CURRENT.internalId + 100, false);

        assertThat(responseFuture.get().isAcknowledged()).isTrue();
    }

    private void subscribeToTablesWithVersion(FutureActionListener<AcknowledgedResponse> responseFuture,
                                              int internalVersionId,
                                              boolean versionInRelation) throws Exception {
        var transportCreateSubscription = new TransportCreateSubscription(
            mock(TransportService.class),
            clusterService,
            logicalReplicationService,
            mock(ThreadPool.class),
            roles
        );

        final DiscoveryNode dataNode = new DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState
            .builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(dataNode).localNodeId(dataNode.getId()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        doAnswer(invocation -> responseFuture.complete(new AcknowledgedResponse(true)))
            .when(clusterService).submitStateUpdateTask(anyString(), any());

        RelationName relationName = RelationName.of(QualifiedName.of("t1"), Schemas.DOC_SCHEMA_NAME);

        // Not necessary to set a valid version, only fact that version is higher than CURRENT is important for this test.
        Settings relationSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, versionInRelation ? internalVersionId : Version.CURRENT.internalId)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2) // must be supplied
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) // must be supplied
            .build();

        String indexUUID = UUIDs.randomBase64UUID();
        Settings.Builder indexSettings = Settings.builder().put(relationSettings).put(SETTING_INDEX_UUID, indexUUID);
        if (!versionInRelation) {
            indexSettings.put(IndexMetadata.SETTING_VERSION_CREATED, internalVersionId);
        }

        Metadata publisherMetadata = Metadata.builder()
            .setTable(
                relationName,
                List.of(),
                relationSettings,
                null,
                ColumnPolicy.STRICT,
                null,
                Map.of(),
                List.of(),
                List.of(),
                IndexMetadata.State.OPEN,
                List.of(indexUUID),
                0L
                )
            .put(IndexMetadata.builder(relationName.indexNameOrAlias()).settings(indexSettings))
            .build();

        PublicationsStateAction.Response response = new PublicationsStateAction.Response(publisherMetadata, List.of());
        when(logicalReplicationService.getPublicationState(anyString(), any(List.class), any(ConnectionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        transportCreateSubscription.masterOperation(
            new CreateSubscriptionRequest(
                "crate",
                "dummy",
                new ConnectionInfo(List.of(), Settings.EMPTY),
                List.of(),
                Settings.EMPTY
            ),
            clusterService.state(),
            responseFuture
        );
    }
}
