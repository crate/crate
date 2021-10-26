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

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.concurrent.CompletableFutures;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Singleton
public class SysSnapshots {

    private static final Logger LOGGER = LogManager.getLogger(SysSnapshots.class);
    private final Supplier<Collection<Repository>> getRepositories;

    @Inject
    public SysSnapshots(RepositoriesService repositoriesService) {
        this(repositoriesService::getRepositoriesList);
    }

    @VisibleForTesting
    SysSnapshots(Supplier<Collection<Repository>> getRepositories) {
        this.getRepositories = getRepositories;
    }

    public CompletableFuture<Iterable<SysSnapshot>> currentSnapshots() {
        ArrayList<CompletableFuture<Collection<SysSnapshot>>> futureActionListeners = new ArrayList<>();
        for (Repository repository : getRepositories.get()) {
            var future = new CompletableFuture<Collection<SysSnapshot>>();
            ActionListener<RepositoryData> listener = ActionListener.wrap(
                repositoryData -> {
                    Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
                    if (snapshotIds.isEmpty()) {
                        future.complete(List.of());
                        return;
                    }
                    GroupedActionListener<SysSnapshot> snapshotListener = new GroupedActionListener<>(
                        ActionListener.wrap(future::complete, future::completeExceptionally),
                        snapshotIds.size()
                    );
                    for (SnapshotId snapshotId : snapshotIds) {
                        createSysSnapshot(repository, snapshotId, snapshotListener);
                    }
                },
                future::completeExceptionally
            );
            try {
                repository.getRepositoryData(listener);
            } catch (Exception ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Couldn't load repository data. repository={} error={}", repository, ex);
                }
                // ignore - `sys.snapshots` shouldn't fail because of an illegal repository definition
                continue;
            }
            futureActionListeners.add(future);
        }
        return CompletableFutures.allAsList(futureActionListeners).thenApply(data -> {
            ArrayList<SysSnapshot> result = new ArrayList<>();
            for (Collection<SysSnapshot> datum : data) {
                result.addAll(datum);
            }
            return result;
        });
    }

    private static void createSysSnapshot(Repository repository,
                                          SnapshotId snapshotId,
                                          ActionListener<SysSnapshot> listener) {
        repository.getSnapshotInfo(
            snapshotId,
            new ActionListener<>() {
                @Override
                public void onResponse(SnapshotInfo snapshotInfo) {
                    Version version = snapshotInfo.version();
                    listener.onResponse(new SysSnapshot(
                        snapshotId.getName(),
                        repository.getMetadata().name(),
                        snapshotInfo.indices(),
                        snapshotInfo.startTime(),
                        snapshotInfo.endTime(),
                        version == null ? null : version.toString(),
                        snapshotInfo.state().name(),
                        Lists2.map(snapshotInfo.shardFailures(), SnapshotShardFailure::toString)
                    ));
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof SnapshotException) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Couldn't retrieve snapshotId={} error={}", snapshotId, e);
                        }
                        listener.onResponse(new SysSnapshot(
                            snapshotId.getName(),
                            repository.getMetadata().name(),
                            Collections.emptyList(),
                            null,
                            null,
                            null,
                            SnapshotState.FAILED.name(),
                            List.of()
                        ));
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
    }
}
