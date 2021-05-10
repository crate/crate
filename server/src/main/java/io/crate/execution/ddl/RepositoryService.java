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

import io.crate.common.annotations.VisibleForTesting;
import io.crate.exceptions.RepositoryUnknownException;
import io.crate.exceptions.SQLExceptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.RepositoryException;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

@Singleton
public class RepositoryService {

    private static final Logger LOGGER = LogManager.getLogger(RepositoryService.class);

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
    public RepositoryMetadata getRepository(String repositoryName) {
        RepositoriesMetadata repositories = clusterService.state().metadata().custom(RepositoriesMetadata.TYPE);
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

    public CompletableFuture<Long> execute(DeleteRepositoryRequest request) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        deleteRepositoryAction.execute(
            request,
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse deleteRepositoryResponse) {
                    if (!deleteRepositoryResponse.isAcknowledged()) {
                        LOGGER.info("delete repository '{}' not acknowledged", request.name());
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

    public CompletableFuture<Long> execute(PutRepositoryRequest request) {
        final CompletableFuture<Long> result = new CompletableFuture<>();

        putRepositoryAction.execute(request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse putRepositoryResponse) {
                result.complete(1L);
            }

            @Override
            public void onFailure(Exception e) {
                final Throwable t = convertRepositoryException(e);

                // in case the put repo action fails in the verificationPhase the repository got already created
                // but an exception is raised anyway.
                // --> remove the repo and then return the exception to the user
                dropIfExists(request.name(), () -> result.completeExceptionally(t));
            }
        });
        return result;
    }

    private void dropIfExists(String repoName, Runnable callback) {
        RepositoryMetadata repository = getRepository(repoName);
        if (repository == null) {
            callback.run();
        } else {
            execute(new DeleteRepositoryRequest(repoName)).whenComplete(
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

        /*
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
