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

package io.crate.expression.reference.sys.snapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import io.crate.common.annotations.VisibleForTesting;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;

@Singleton
public class SysSnapshots {

    private static final Logger LOGGER = LogManager.getLogger(SysSnapshots.class);
    private final Supplier<Collection<Repository>> getRepositories;
    private final ClusterService clusterService;

    @Inject
    public SysSnapshots(RepositoriesService repositoriesService, ClusterService clusterService) {
        this(repositoriesService::getRepositoriesList, clusterService);
    }

    @VisibleForTesting
    SysSnapshots(Supplier<Collection<Repository>> getRepositories, ClusterService clusterService) {
        this.getRepositories = getRepositories;
        this.clusterService = clusterService;
    }

    public CompletableFuture<Iterable<SysSnapshot>> currentSnapshots() {
        ArrayList<CompletableFuture<List<SysSnapshot>>> remoteSnapshots = new ArrayList<>();
        ClusterState state = clusterService.state();
        Metadata metadata = state.metadata();
        for (Repository repository : getRepositories.get()) {
            var futureSnapshots = repository.getRepositoryData().thenCompose(repositoryData -> {
                Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
                ArrayList<CompletableFuture<SysSnapshot>> snapshots = new ArrayList<>(snapshotIds.size());
                for (SnapshotId snapshotId : snapshotIds) {
                    snapshots.add(createSysSnapshot(repository, snapshotId));
                }
                return CompletableFutures.allSuccessfulAsList(snapshots);
            });
            remoteSnapshots.add(futureSnapshots);
        }
        return CompletableFutures.allSuccessfulAsList(remoteSnapshots).thenApply(data -> {
            ArrayList<SysSnapshot> result = new ArrayList<>();
            for (Collection<SysSnapshot> datum : data) {
                result.addAll(datum);
            }
            for (var inProgressEntry : state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
                result.add(SysSnapshot.of(metadata, inProgressEntry));
            }
            return result;
        });
    }

    private static CompletableFuture<SysSnapshot> createSysSnapshot(Repository repository, SnapshotId snapshotId) {
        String repoName = repository.getMetadata().name();
        return repository.getSnapshotGlobalMetadata(snapshotId)
            .thenCombine(repository.getSnapshotInfo(snapshotId), (metadata, info) -> SysSnapshot.of(metadata, repoName, info))
            .exceptionally(t -> {
                var err = SQLExceptions.unwrap(t);
                if (err instanceof SnapshotException) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Couldn't retrieve snapshotId={} error={}", snapshotId, err);
                    }
                    return SysSnapshot.ofMissingInfo(repository.getMetadata().name(), snapshotId);
                }
                throw Exceptions.toRuntimeException(err);
            });
    }
}
