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
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
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
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class RepositoryServiceTest extends CrateUnitTest {

    private ThreadPool threadPool;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new ThreadPool("dummy");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void testConvertException() throws Throwable {
        expectedException.expect(RepositoryException.class);
        expectedException.expectMessage("[foo] failed: [foo] missing location");

        throw RepositoryService.convertRepositoryException(new RepositoryException("foo", "failed", new CreationException(ImmutableList.of(
                new Message(Collections.<Object>singletonList(10),
                        "creation error", new RepositoryException("foo", "missing location"))
        ))));
    }

    @Test
    public void testRepositoryIsDroppedOnFailure() throws Throwable  {
        expectedException.expect(RepositoryException.class);

        // add repo to cluster service so that it exists..
        RepositoriesMetaData repos = new RepositoriesMetaData(new RepositoryMetaData("repo1", "fs", Settings.EMPTY));
        ClusterState state = ClusterState.builder(new ClusterName("dummy")).metaData(
                MetaData.builder().putCustom(RepositoriesMetaData.TYPE, repos)).build();
        ClusterService clusterService = new NoopClusterService(state);

        TransportActionProvider transportActionProvider = mock(TransportActionProvider.class);

        final ActionFilters actionFilters = mock(ActionFilters.class, Answers.RETURNS_MOCKS.get());
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);
        TransportDeleteRepositoryAction deleteRepositoryAction = new TransportDeleteRepositoryAction(
                Settings.EMPTY,
                mock(TransportService.class, Answers.RETURNS_MOCKS.get()),
                clusterService,
                mock(RepositoriesService.class),
                threadPool,
                actionFilters,
                indexNameExpressionResolver) {
            @Override
            protected void doExecute(Task task, DeleteRepositoryRequest request, ActionListener<DeleteRepositoryResponse> listener) {
                listener.onResponse(mock(DeleteRepositoryResponse.class));
            }
        };
        when(transportActionProvider.transportDeleteRepositoryAction()).thenReturn(deleteRepositoryAction);

        TransportPutRepositoryAction putRepo = new TransportPutRepositoryAction(
                Settings.EMPTY,
                mock(TransportService.class, Answers.RETURNS_MOCKS.get()),
                clusterService,
                mock(RepositoriesService.class),
                threadPool,
                actionFilters,
                indexNameExpressionResolver) {
            @Override
            protected void doExecute(Task task, PutRepositoryRequest request, ActionListener<PutRepositoryResponse> listener) {
                listener.onFailure(new RepositoryException(request.name(), "failure"));
            }
        };
        when(transportActionProvider.transportPutRepositoryAction()).thenReturn(putRepo);

        RepositoryService repositoryService = new RepositoryService(clusterService, transportActionProvider);
        try {
            repositoryService.execute(
                    new CreateRepositoryAnalyzedStatement("repo1", "fs", Settings.EMPTY)).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            verify(transportActionProvider, times(1)).transportDeleteRepositoryAction();
            throw e.getCause();
        }
    }
}
