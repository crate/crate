/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.ddl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.netty.NettyBootstrap;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class RepositoryServiceTest extends CrateDummyClusterServiceUnitTest {

    private NettyBootstrap nettyBootstrap;

    @Before
    public void setupNetty() {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
    }

    @After
    public void teardownNetty() {
        nettyBootstrap.close();
    }

    @Test
    public void testConvertException() throws Throwable {
        Throwable result = RepositoryService.convertRepositoryException(
            new RepositoryException("foo", "failed", new CreationException(
                List.of(new Message(
                    Collections.singletonList(10),
                    "creation error",
                    new RepositoryException("foo", "missing location"))
                ))));
        assertThat(result).isExactlyInstanceOf(RepositoryException.class)
            .hasMessage("[] [foo] failed: [foo] missing location");
    }

    @Test
    public void testRepositoryIsDroppedOnFailure() throws Throwable {
        // add repo to cluster service so that it exists..
        RepositoriesMetadata repos = new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata("repo1", "fs", Settings.EMPTY)));
        ClusterState state = ClusterState.builder(new ClusterName("dummy")).metadata(
                Metadata.builder().putCustom(RepositoriesMetadata.TYPE, repos)).build();
        ClusterServiceUtils.setState(clusterService, state);


        final AtomicBoolean deleteRepoCalled = new AtomicBoolean(false);
        MockTransportService transportService = MockTransportService.createNewService(
            Settings.EMPTY, Version.CURRENT, THREAD_POOL, nettyBootstrap, clusterService.getClusterSettings());
        TransportDeleteRepositoryAction deleteRepositoryAction = new TransportDeleteRepositoryAction(
            transportService,
            clusterService,
            mock(RepositoriesService.class),
            THREAD_POOL) {
            @Override
            protected void doExecute(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
                deleteRepoCalled.set(true);
                listener.onResponse(mock(AcknowledgedResponse.class));
            }
        };

        TransportPutRepositoryAction putRepo = new TransportPutRepositoryAction(
            transportService,
            clusterService,
            mock(RepositoriesService.class),
            THREAD_POOL) {
            @Override
            protected void doExecute(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
                listener.onFailure(new RepositoryException(request.name(), "failure"));
            }
        };

        NodeClient nodeClient = new NodeClient(Settings.EMPTY, THREAD_POOL);
        nodeClient.initialize(Map.of(
            PutRepositoryAction.INSTANCE, putRepo,
            DeleteRepositoryAction.INSTANCE, deleteRepositoryAction
        ));

        RepositoryService repositoryService = new RepositoryService(clusterService, nodeClient);
        PutRepositoryRequest request = new PutRepositoryRequest("repo1");
        request.type("fs");
        assertThatThrownBy(() -> repositoryService.execute(request).get(10, TimeUnit.SECONDS))
            .isExactlyInstanceOf(ExecutionException.class)
            .hasCauseExactlyInstanceOf(RepositoryException.class);
        assertThat(deleteRepoCalled.get()).isTrue();
    }
}
