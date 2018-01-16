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

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.analyze.DropRepositoryAnalyzedStatement;
import io.crate.exceptions.RepositoryUnknownException;
import io.crate.exceptions.SQLExceptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.repositories.RepositoryException;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

@Singleton
public class RepositoryService {

    private static final Logger LOGGER = Loggers.getLogger(RepositoryService.class);

    private final ClusterService clusterService;
    private final TransportDeleteRepositoryAction deleteRepositoryAction;
    private final TransportPutRepositoryAction putRepositoryAction;

    @Inject
    public RepositoryService(ClusterService clusterService,
                             TransportDeleteRepositoryAction deleteRepositoryAction,
                             TransportPutRepositoryAction putRepositoryAction) {
        this.clusterService = clusterService;
        this.deleteRepositoryAction = deleteRepositoryAction;
        this.putRepositoryAction = putRepositoryAction;
    }

    @Nullable
    public RepositoryMetaData getRepository(String repositoryName) {
        RepositoriesMetaData repositories = clusterService.state().metaData().custom(RepositoriesMetaData.TYPE);
        if (repositories != null) {
            return repositories.repository(repositoryName);
        }
        return null;
    }

    public void failIfRepositoryDoesNotExist(String repositoryName) {
        if (getRepository(repositoryName) == null) {
            throw new RepositoryUnknownException(repositoryName);
        }
    }

    public CompletableFuture<Long> execute(DropRepositoryAnalyzedStatement analyzedStatement) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        final String repoName = analyzedStatement.repositoryName();
        deleteRepositoryAction.execute(
            new DeleteRepositoryRequest(repoName),
            new ActionListener<DeleteRepositoryResponse>() {
                @Override
                public void onResponse(DeleteRepositoryResponse deleteRepositoryResponse) {
                    if (!deleteRepositoryResponse.isAcknowledged()) {
                        LOGGER.info("delete repository '{}' not acknowledged", repoName);
                    }
                    future.complete(1L);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        return future;
    }

    public CompletableFuture<Long> execute(CreateRepositoryAnalyzedStatement statement) {
        final CompletableFuture<Long> result = new CompletableFuture<>();
        final String repoName = statement.repositoryName();

        PutRepositoryRequest request = new PutRepositoryRequest(repoName);
        request.type(statement.repositoryType());
        request.settings(statement.settings());
        putRepositoryAction.execute(request, new ActionListener<PutRepositoryResponse>() {
            @Override
            public void onResponse(PutRepositoryResponse putRepositoryResponse) {
                result.complete(1L);
            }

            @Override
            public void onFailure(Exception e) {
                final Throwable t = convertRepositoryException(e);

                // in case the put repo action fails in the verificationPhase the repository got already created
                // but an exception is raised anyway.
                // --> remove the repo and then return the exception to the user
                dropIfExists(repoName, new Runnable() {
                    @Override
                    public void run() {
                        result.completeExceptionally(t);
                    }
                });
            }
        });
        return result;
    }

    private void dropIfExists(String repoName, Runnable callback) {
        RepositoryMetaData repository = getRepository(repoName);
        if (repository == null) {
            callback.run();
        } else {
            execute(new DropRepositoryAnalyzedStatement(repoName)).whenComplete(
                (Long result, Throwable t) -> {
                    if (t != null) {
                        LOGGER.error("Error occurred whilst trying to delete repository", t);
                    }
                    callback.run();
                }
            );
        }
    }

    @VisibleForTesting
    static Throwable convertRepositoryException(Throwable e) {
        e = SQLExceptions.unwrap(e);
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
            return new RepositoryException("", msg, e);
        } else {
            return e;
        }
    }
}
