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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.collections.Lists;
import io.crate.common.concurrent.CompletableFutures;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.metadata.RelationName;

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
        ArrayList<CompletableFuture<List<SysSnapshot>>> sysSnapshots = new ArrayList<>();
        for (Repository repository : getRepositories.get()) {
            var futureSnapshots = repository.getRepositoryData()
                .thenCompose(repositoryData -> {
                    Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
                    ArrayList<CompletableFuture<SysSnapshot>> snapshots = new ArrayList<>(snapshotIds.size());
                    for (SnapshotId snapshotId : snapshotIds) {
                        snapshots.add(createSysSnapshot(repository, snapshotId));
                    }
                    return CompletableFutures.allSuccessfulAsList(snapshots);
                });
            sysSnapshots.add(futureSnapshots);

            // Add snapshots in progress in this repository.
            final SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE);
            List<SysSnapshot> inProgressSnapshots = snapshotsInProgress(snapshotsInProgress, repository.getMetadata().name());
            sysSnapshots.add(CompletableFuture.completedFuture(inProgressSnapshots));
        }
        return CompletableFutures.allSuccessfulAsList(sysSnapshots).thenApply(data -> {
            ArrayList<SysSnapshot> result = new ArrayList<>();
            for (Collection<SysSnapshot> datum : data) {
                result.addAll(datum);
            }
            return result;
        });
    }

    private static SysSnapshot toSysSnapshot(Repository repository,
                                             SnapshotId snapshotId,
                                             SnapshotInfo snapshotInfo,
                                             List<String> partedTables) {
        Version version = snapshotInfo.version();
        return new SysSnapshot(
            snapshotId.getName(),
            repository.getMetadata().name(),
            snapshotInfo.indices(),
            partedTables,
            snapshotInfo.startTime(),
            snapshotInfo.endTime(),
            version == null ? null : version.toString(),
            snapshotInfo.state().name(),
            Lists.map(snapshotInfo.shardFailures(), SnapshotShardFailure::toString)
        );
    }

    private static SysSnapshot toSysSnapshot(String repositoryName,
                                             SnapshotsInProgress.Entry entry,
                                             List<String> partedTables) {
        return new SysSnapshot(
            entry.snapshot().getSnapshotId().getName(),
            repositoryName,
            entry.indices().stream().map(IndexId::getName).toList(),
            partedTables,
            entry.startTime(),
            0L,
            Version.CURRENT.toString(),
            SnapshotState.IN_PROGRESS.name(),
            Collections.emptyList()
        );
    }

    private static CompletableFuture<SysSnapshot> createSysSnapshot(Repository repository, SnapshotId snapshotId) {
        return repository.getSnapshotGlobalMetadata(snapshotId).thenCombine(
            repository.getSnapshotInfo(snapshotId),
            (metadata, snapshotInfo) -> {
                List<String> partedTables = new ArrayList<>();
                for (var template : metadata.templates().values()) {
                    partedTables.add(RelationName.fqnFromIndexName(template.value.name()));
                }
                return SysSnapshots.toSysSnapshot(repository, snapshotId, snapshotInfo, partedTables);
            }).exceptionally(t -> {
                var err = SQLExceptions.unwrap(t);
                if (err instanceof SnapshotException) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Couldn't retrieve snapshotId={} error={}", snapshotId, err);
                    }
                    return new SysSnapshot(
                        snapshotId.getName(),
                        repository.getMetadata().name(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        SnapshotState.FAILED.name(),
                        List.of()
                    );
                }
                throw Exceptions.toRuntimeException(err);
            });
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName repository to check running snapshots
     * @return list of snapshots
     */
    public static List<SysSnapshot> snapshotsInProgress(@Nullable SnapshotsInProgress snapshotsInProgress,
                                                        String repositoryName) {
        List<SysSnapshot> sysSnapshots = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries =
            SnapshotsService.currentSnapshots(snapshotsInProgress, repositoryName, Collections.emptyList());
        for (SnapshotsInProgress.Entry entry : entries) {
            List<String> partedTables = new ArrayList<>();
            for (var template : entry.templates()) {
                partedTables.add(RelationName.fqnFromIndexName(template));
            }
            sysSnapshots.add(SysSnapshots.toSysSnapshot(repositoryName, entry, partedTables));
        }
        return sysSnapshots;
    }
}
