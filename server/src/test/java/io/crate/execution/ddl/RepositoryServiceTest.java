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

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class RepositoryServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testConvertException() throws Throwable {
        expectedException.expect(RepositoryException.class);
        expectedException.expectMessage("[foo] failed: [foo] missing location");

        throw RepositoryService.convertRepositoryException(
            new RepositoryException("foo", "failed", new CreationException(
                List.of(new Message(
                    Collections.singletonList(10),
                    "creation error",
                    new RepositoryException("foo", "missing location"))
                ))));
    }

    @Test
    public void testRepositoryIsDroppedOnFailure() throws Throwable {
        expectedException.expect(RepositoryException.class);

        // add repo to cluster service so that it exists..
        RepositoriesMetadata repos = new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata("repo1", "fs", Settings.EMPTY)));
        ClusterState state = ClusterState.builder(new ClusterName("dummy")).metadata(
                Metadata.builder().putCustom(RepositoriesMetadata.TYPE, repos)).build();
        ClusterServiceUtils.setState(clusterService, state);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();


        final AtomicBoolean deleteRepoCalled = new AtomicBoolean(false);
        TransportDeleteRepositoryAction deleteRepositoryAction = new TransportDeleteRepositoryAction(
            MockTransportService.createNewService(
                Settings.EMPTY, Version.CURRENT, THREAD_POOL, clusterService.getClusterSettings()),
            clusterService,
            mock(RepositoriesService.class),
            THREAD_POOL,
            indexNameExpressionResolver) {
            @Override
            protected void doExecute(Task task, DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
                deleteRepoCalled.set(true);
                listener.onResponse(mock(AcknowledgedResponse.class));
            }
        };

        TransportPutRepositoryAction putRepo = new TransportPutRepositoryAction(
            MockTransportService.createNewService(
                Settings.EMPTY, Version.CURRENT, THREAD_POOL, clusterService.getClusterSettings()),
            clusterService,
            mock(RepositoriesService.class),
            THREAD_POOL,
            indexNameExpressionResolver) {
            @Override
            protected void doExecute(Task task, PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
                listener.onFailure(new RepositoryException(request.name(), "failure"));
            }
        };

        RepositoryService repositoryService = new RepositoryService(
            clusterService,
            deleteRepositoryAction,
            putRepo);
        try {
            PutRepositoryRequest request = new PutRepositoryRequest("repo1");
            request.type("fs");
            repositoryService.execute(request).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(deleteRepoCalled.get(), is(true));
            throw e.getCause();
        }
    }
}
