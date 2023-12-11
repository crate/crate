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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.action.FutureActionListener;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.RelationMetadata;
import io.crate.sql.tree.QualifiedName;
import io.crate.role.Role;
import io.crate.role.RoleLookup;

public class TransportCreateSubscriptionActionTest {

    private final LogicalReplicationService logicalReplicationService = mock(LogicalReplicationService.class);
    private final RoleLookup userLookup = mock(RoleLookup.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private TransportCreateSubscriptionAction transportCreateSubscriptionAction;


    @Test
    public void test_subscribing_to_tables_with_higher_version_raises_error() throws Exception {
        FutureActionListener<AcknowledgedResponse> responseFuture = new FutureActionListener<>();
        subscribeToTablesWithVersion(responseFuture, Version.CURRENT.internalId + 10000);

        var tableVersion = Version.fromId(Version.CURRENT.internalId + 10000);
        assertThatThrownBy(responseFuture::get)
            .hasMessageContaining("One of the published tables has version higher than subscriber's minimal node version." +
                " Table=doc.t1, Table-Version=" + tableVersion + ", Local-Minimal-Version: " + Version.CURRENT);
    }

    @Test
    public void test_subscribing_to_tables_with_higher_hotfix_works() throws Exception {
        FutureActionListener<AcknowledgedResponse> responseFuture = new FutureActionListener<>();
        subscribeToTablesWithVersion(responseFuture, Version.CURRENT.internalId + 100);

        assertThat(responseFuture.get().isAcknowledged()).isTrue();
    }

    private void subscribeToTablesWithVersion(FutureActionListener<AcknowledgedResponse> responseFuture,
                                              int internalVersionId) throws Exception {
        transportCreateSubscriptionAction = new TransportCreateSubscriptionAction(
            mock(TransportService.class),
            clusterService,
            logicalReplicationService,
            mock(ThreadPool.class),
            userLookup
        );

        when(userLookup.findUser(anyString())).thenReturn(Role.CRATE_USER);

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
        RelationMetadata relationMetadata = new RelationMetadata(
            relationName,
            List.of(
                IndexMetadata.builder("t1")
                .settings(
                    // Not necessary to set a valid version, only fact that version is higher than CURRENT is important for this test.
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, internalVersionId)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2) // must be supplied
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) // must be supplied
                )
                .build()
            ),
            null
        );

        PublicationsStateAction.Response response = new PublicationsStateAction.Response(
            Map.of(relationName, relationMetadata),
            List.of()
        );
        when(logicalReplicationService.getPublicationState(anyString(), any(List.class), any(ConnectionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        transportCreateSubscriptionAction.masterOperation(
            new CreateSubscriptionRequest(
                "dummy",
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
