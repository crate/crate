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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.DropRepositoryAnalyzedStatement;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.UUID;

@Singleton
public class RepositoryDDLDispatcher {

    private static final ESLogger LOGGER = Loggers.getLogger(RepositoryDDLDispatcher.class);

    private final TransportDeleteRepositoryAction transportDeleteRepositoryAction;

    @Inject
    public RepositoryDDLDispatcher(TransportDeleteRepositoryAction transportDeleteRepositoryAction) {
        this.transportDeleteRepositoryAction = transportDeleteRepositoryAction;
    }

    public ListenableFuture<Long> dispatch(DropRepositoryAnalyzedStatement analyzedStatement, UUID jobId) {
        final SettableFuture<Long> future = SettableFuture.create();
        final String repoName = analyzedStatement.repositoryName();
        transportDeleteRepositoryAction.execute(
                new DeleteRepositoryRequest(repoName),
                new ActionListener<DeleteRepositoryResponse>() {
                    @Override
                    public void onResponse(DeleteRepositoryResponse deleteRepositoryResponse) {
                        if (!deleteRepositoryResponse.isAcknowledged()) {
                            LOGGER.info("delete repository '{}' not acknowledged", repoName);
                        }
                        future.set(1L);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        future.setException(e);
                    }
                }
        );
        return future;
    }
}
