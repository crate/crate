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

package io.crate.execution.ddl;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.es.Version;
import io.crate.es.action.ActionListener;
import io.crate.es.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import io.crate.es.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import io.crate.es.action.admin.cluster.repositories.put.PutRepositoryRequest;
import io.crate.es.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.cluster.ClusterName;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.metadata.MetaData;
import io.crate.es.cluster.metadata.RepositoriesMetaData;
import io.crate.es.cluster.metadata.RepositoryMetaData;
import io.crate.es.common.inject.CreationException;
import io.crate.es.common.inject.spi.Message;
import io.crate.es.common.settings.Settings;
import io.crate.es.repositories.RepositoriesService;
import io.crate.es.repositories.RepositoryException;
import io.crate.es.tasks.Task;
import io.crate.es.test.ClusterServiceUtils;
import io.crate.es.test.transport.MockTransportService;
import org.junit.Test;

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
        RepositoriesMetaData repos = new RepositoriesMetaData(Collections.singletonList(new RepositoryMetaData("repo1", "fs", Settings.EMPTY)));
        ClusterState state = ClusterState.builder(new ClusterName("dummy")).metaData(
            MetaData.builder().putCustom(RepositoriesMetaData.TYPE, repos)).build();
        ClusterServiceUtils.setState(clusterService, state);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);


        final AtomicBoolean deleteRepoCalled = new AtomicBoolean(false);
        TransportDeleteRepositoryAction deleteRepositoryAction = new TransportDeleteRepositoryAction(
            Settings.EMPTY,
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
            Settings.EMPTY,
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
            repositoryService.execute(
                new CreateRepositoryAnalyzedStatement("repo1", "fs", Settings.EMPTY)).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(deleteRepoCalled.get(), is(true));
            throw e.getCause();
        }
    }
}
