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

import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;

@Singleton
public class RepositoryService {

    private static final Logger LOGGER = LogManager.getLogger(RepositoryService.class);

    private final ClusterService clusterService;

    private final NodeClient client;

    public RepositoryService(ClusterService clusterService, NodeClient client) {
        this.clusterService = clusterService;
        this.client = client;
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
            throw new RepositoryMissingException(repositoryName);
        }
    }

    public CompletableFuture<Long> execute(DeleteRepositoryRequest request) {
        return client.execute(DeleteRepositoryAction.INSTANCE, request).thenApply(response -> {
            if (!response.isAcknowledged()) {
                LOGGER.info("delete repository '{}' not acknowledged", request.name());
            }
            return 1L;
        });
    }

    public CompletableFuture<Long> execute(PutRepositoryRequest request) {
        return client.execute(PutRepositoryAction.INSTANCE, request).thenApply(r -> 1L)
            .exceptionallyCompose(err -> {
                final Throwable t = convertRepositoryException(err);
                // in case the put repo action fails in the verificationPhase the repository got already created
                // but an exception is raised anyway.
                // --> remove the repo and then return the exception to the user
                return dropIfExists(request.name()).thenApply(ignored -> {
                    throw Exceptions.toRuntimeException(t);
                });
            });
    }

    private CompletableFuture<Long> dropIfExists(String repoName) {
        RepositoryMetadata repository = getRepository(repoName);
        if (repository == null) {
            return CompletableFuture.completedFuture(1L);
        }
        return execute(new DeleteRepositoryRequest(repoName))
            .exceptionally(err -> {
                LOGGER.error("Error occurred whilst trying to delete repository", err);
                return -1L;
            });
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
