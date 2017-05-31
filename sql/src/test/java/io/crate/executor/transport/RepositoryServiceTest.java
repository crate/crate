/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Collections;
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

        throw RepositoryService.convertRepositoryException(new RepositoryException("foo", "failed", new CreationException(ImmutableList.of(
            new Message(Collections.singletonList(10),
                "creation error", new RepositoryException("foo", "missing location"))
        ))));
    }

    @Test
    public void testRepositoryIsDroppedOnFailure() throws Throwable {
        expectedException.expect(RepositoryException.class);

        // add repo to cluster service so that it exists..
        RepositoriesMetaData repos = new RepositoriesMetaData(new RepositoryMetaData("repo1", "fs", Settings.EMPTY));
        ClusterState state = ClusterState.builder(new ClusterName("dummy")).metaData(
            MetaData.builder().putCustom(RepositoriesMetaData.TYPE, repos)).build();
        ClusterServiceUtils.setState(clusterService, state);
        final ActionFilters actionFilters = mock(ActionFilters.class, Answers.RETURNS_MOCKS.get());
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);


        final AtomicBoolean deleteRepoCalled = new AtomicBoolean(false);
        TransportDeleteRepositoryAction deleteRepositoryAction = new TransportDeleteRepositoryAction(
            Settings.EMPTY,
            MockTransportService.local(Settings.EMPTY, Version.V_5_0_1, THREAD_POOL, clusterService.getClusterSettings()),
            clusterService,
            mock(RepositoriesService.class),
            THREAD_POOL,
            actionFilters,
            indexNameExpressionResolver) {
            @Override
            protected void doExecute(Task task, DeleteRepositoryRequest request, ActionListener<DeleteRepositoryResponse> listener) {
                deleteRepoCalled.set(true);
                listener.onResponse(mock(DeleteRepositoryResponse.class));
            }
        };

        TransportPutRepositoryAction putRepo = new TransportPutRepositoryAction(
            Settings.EMPTY,
            MockTransportService.local(Settings.EMPTY, Version.V_5_0_1, THREAD_POOL, clusterService.getClusterSettings()),
            clusterService,
            mock(RepositoriesService.class),
            THREAD_POOL,
            actionFilters,
            indexNameExpressionResolver) {
            @Override
            protected void doExecute(Task task, PutRepositoryRequest request, ActionListener<PutRepositoryResponse> listener) {
                listener.onFailure(new RepositoryException(request.name(), "failure"));
            }
        };

        RepositoryService repositoryService = new RepositoryService(
            clusterService,
            deleteRepositoryAction,
            putRepo);
        try {
            repositoryService.execute(
                new CreateRepositoryAnalyzedStatement("repo1", "fs", Settings.EMPTY)).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(deleteRepoCalled.get(), is(true));
            throw e.getCause();
        }
    }
}
