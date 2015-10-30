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
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.analyze.DropRepositoryAnalyzedStatement;
import io.crate.exceptions.Exceptions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.repositories.RepositoryException;

@Singleton
public class RepositoryDDLDispatcher {

    private static final ESLogger LOGGER = Loggers.getLogger(RepositoryDDLDispatcher.class);

    private final TransportActionProvider transportActionProvider;

    @Inject
    public RepositoryDDLDispatcher(TransportActionProvider transportActionProvider) {
        this.transportActionProvider = transportActionProvider;

    }

    public ListenableFuture<Long> dispatch(DropRepositoryAnalyzedStatement analyzedStatement) {
        final SettableFuture<Long> future = SettableFuture.create();
        final String repoName = analyzedStatement.repositoryName();
        transportActionProvider.transportDeleteRepositoryAction().execute(
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

    public ListenableFuture<Long> dispatch(CreateRepositoryAnalyzedStatement statement) {
        final SettableFuture<Long> result = SettableFuture.create();

        PutRepositoryRequest request = new PutRepositoryRequest(statement.repositoryName());
        request.type(statement.repositoryType());
        request.settings(statement.settings());
        transportActionProvider.transportPutRepositoryAction().execute(request, new PutRepositoryResponseActionListener(result));
        return result;
    }

    static class PutRepositoryResponseActionListener implements ActionListener<PutRepositoryResponse> {
        private final SettableFuture<Long> result;

        public PutRepositoryResponseActionListener(SettableFuture<Long> result) {
            this.result = result;
        }

        @Override
        public void onResponse(PutRepositoryResponse putRepositoryResponse) {
            result.set(1L);
        }

        @Override
        public void onFailure(Throwable e) {
            e = Exceptions.unwrap(e);
            Throwable cause = e.getCause();

            /**
             * usually the exception looks like:
             *      RepositoryException
             *          cause: CreationException (from guice)
             *                      cause: RepositoryException (with a message that includes the real failure reason
             *
             * results in something like: [foo] failed to create repository: [foo] missing location
             * instead of just: [foo] failed to create repository
             */
            if (e instanceof RepositoryException && cause != null) {
                String msg = e.getMessage();
                if (cause instanceof CreationException && cause.getCause() != null) {
                    msg += ": " + cause.getCause().getMessage();
                }
                result.setException(new RepositoryException("", msg, e));
            } else {
                result.setException(e);
            }
        }
    }
}
