/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import static java.util.Collections.unmodifiableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.common.concurrent.CompletableFutures;
import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;

/**
 * Transport Action for get snapshots operation
 */
public class TransportGetSnapshotsAction extends TransportMasterNodeAction<GetSnapshotsRequest, GetSnapshotsResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportGetSnapshotsAction.class);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetSnapshotsAction(TransportService transportService,
                                       ClusterService clusterService,
                                       ThreadPool threadPool,
                                       RepositoriesService repositoriesService) {
        super(
            GetSnapshotsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            GetSnapshotsRequest::new
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected GetSnapshotsResponse read(StreamInput in) throws IOException {
        return new GetSnapshotsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshotsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(final GetSnapshotsRequest request,
                                   final ClusterState state,
                                   final ActionListener<GetSnapshotsResponse> listener) {
        String repository = request.repository();
        var futureRepositoryData = isCurrentSnapshotsOnly(request.snapshots())
            ? CompletableFuture.<RepositoryData>completedFuture(null)
            : repositoriesService.getRepositoryData(repository);
        futureRepositoryData.whenComplete((repositoryData, err) -> {
            if (err != null) {
                listener.onFailure(Exceptions.toException(SQLExceptions.unwrap(err)));
                return;
            }
            try {
                masterOperationWithRepositoryData(request, state, listener, repositoryData);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void masterOperationWithRepositoryData(final GetSnapshotsRequest request,
                                                   final ClusterState state,
                                                   final ActionListener<GetSnapshotsResponse> listener,
                                                   final RepositoryData repositoryData) {
        try {
            final String repository = request.repository();
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            final Map<String, SnapshotId> allSnapshotIds = new HashMap<>();
            final List<SnapshotInfo> currentSnapshots = new ArrayList<>();
            for (SnapshotInfo snapshotInfo : sortedCurrentSnapshots(snapshotsInProgress, repository)) {
                SnapshotId snapshotId = snapshotInfo.snapshotId();
                allSnapshotIds.put(snapshotId.getName(), snapshotId);
                currentSnapshots.add(snapshotInfo);
            }
            if (repositoryData != null) {
                for (SnapshotId snapshotId : repositoryData.getSnapshotIds()) {
                    allSnapshotIds.put(snapshotId.getName(), snapshotId);
                }
            }

            final Set<SnapshotId> toResolve = new HashSet<>();
            if (isAllSnapshots(request.snapshots())) {
                toResolve.addAll(allSnapshotIds.values());
            } else {
                for (String snapshotOrPattern : request.snapshots()) {
                    if (GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshotOrPattern)) {
                        toResolve.addAll(currentSnapshots.stream().map(SnapshotInfo::snapshotId).collect(Collectors.toList()));
                    } else if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                        if (allSnapshotIds.containsKey(snapshotOrPattern)) {
                            toResolve.add(allSnapshotIds.get(snapshotOrPattern));
                        } else if (request.ignoreUnavailable() == false) {
                            throw new SnapshotMissingException(repository, snapshotOrPattern);
                        }
                    } else {
                        for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
                            if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                                toResolve.add(entry.getValue());
                            }
                        }
                    }
                }

                if (toResolve.isEmpty() && request.ignoreUnavailable() == false && isCurrentSnapshotsOnly(request.snapshots()) == false) {
                    throw new SnapshotMissingException(repository, request.snapshots()[0]);
                }
            }

            if (request.verbose()) {
                snapshots(
                    snapshotsInProgress,
                    repository,
                    new ArrayList<>(toResolve),
                    request.ignoreUnavailable(),
                    listener.map(snapshotInfos -> new GetSnapshotsResponse(new ArrayList<>(snapshotInfos)))
                );
            } else {
                final List<SnapshotInfo> snapshotInfos;
                if (repositoryData != null) {
                    // want non-current snapshots as well, which are found in the repository data
                    snapshotInfos = buildSimpleSnapshotInfos(toResolve, repositoryData, currentSnapshots);
                } else {
                    // only want current snapshots
                    snapshotInfos = currentSnapshots.stream().map(SnapshotInfo::basic).collect(Collectors.toList());
                    CollectionUtil.timSort(snapshotInfos);
                }
                listener.onResponse(new GetSnapshotsResponse(snapshotInfos));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName repository name
     * @return list of snapshots
     */
    private static List<SnapshotInfo> sortedCurrentSnapshots(@Nullable SnapshotsInProgress snapshotsInProgress, String repositoryName) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries =
                SnapshotsService.currentSnapshots(snapshotsInProgress, repositoryName, Collections.emptyList());
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(new SnapshotInfo(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return unmodifiableList(snapshotList);
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName      repository name
     * @param snapshotIds         snapshots for which to fetch snapshot information
     * @param ignoreUnavailable   if true, snapshots that could not be read will only be logged with a warning,
     *                            if false, they will throw an error
     * @return list of snapshots
     */
    public void snapshots(@Nullable SnapshotsInProgress snapshotsInProgress,
                          String repositoryName,
                          List<SnapshotId> snapshotIds,
                          boolean ignoreUnavailable,
                          ActionListener<Collection<SnapshotInfo>> listener) {
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress, repositoryName, snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList()));
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotSet.add(new SnapshotInfo(entry));
            snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId());
        }
        // then, look in the repository
        final Repository repository = repositoriesService.repository(repositoryName);
        List<CompletableFuture<SnapshotInfo>> futureSnapshotInfos = new ArrayList<>(snapshotIdsToIterate.size());
        for (SnapshotId snapshotId : snapshotIdsToIterate) {
            if (ignoreUnavailable) {
                futureSnapshotInfos.add(repository.getSnapshotInfo(snapshotId).exceptionally(ex -> {
                    LOGGER.warn(() -> new ParameterizedMessage("failed to get snapshot [{}]", snapshotId), ex);
                    return null;
                }));
            } else {
                futureSnapshotInfos.add(repository.getSnapshotInfo(snapshotId));
            }
        }
        CompletableFutures.allAsList(futureSnapshotInfos).whenComplete((snapshotInfos, e) -> {
            if (e != null) {
                listener.onFailure(Exceptions.toException(SQLExceptions.unwrap(e)));
                return;
            }
            ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotSet);
            for (SnapshotInfo snapshotInfo : snapshotInfos) {
                if (snapshotInfo != null) {
                    snapshotList.add(snapshotInfo);
                }
            }
            CollectionUtil.timSort(snapshotList);
            listener.onResponse(unmodifiableList(snapshotList));
        });
    }

    private boolean isAllSnapshots(String[] snapshots) {
        return (snapshots.length == 0) || (snapshots.length == 1 && GetSnapshotsRequest.ALL_SNAPSHOTS.equalsIgnoreCase(snapshots[0]));
    }

    private boolean isCurrentSnapshotsOnly(String[] snapshots) {
        return (snapshots.length == 1 && GetSnapshotsRequest.CURRENT_SNAPSHOT.equalsIgnoreCase(snapshots[0]));
    }

    private List<SnapshotInfo> buildSimpleSnapshotInfos(final Set<SnapshotId> toResolve,
                                                        final RepositoryData repositoryData,
                                                        final List<SnapshotInfo> currentSnapshots) {
        List<SnapshotInfo> snapshotInfos = new ArrayList<>();
        for (SnapshotInfo snapshotInfo : currentSnapshots) {
            if (toResolve.remove(snapshotInfo.snapshotId())) {
                snapshotInfos.add(snapshotInfo.basic());
            }
        }
        Map<SnapshotId, List<String>> snapshotsToIndices = new HashMap<>();
        for (IndexId indexId : repositoryData.getIndices().values()) {
            for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                if (toResolve.contains(snapshotId)) {
                    snapshotsToIndices.computeIfAbsent(snapshotId, (k) -> new ArrayList<>())
                                      .add(indexId.getName());
                }
            }
        }
        for (Map.Entry<SnapshotId, List<String>> entry : snapshotsToIndices.entrySet()) {
            final List<String> indices = entry.getValue();
            CollectionUtil.timSort(indices);
            final SnapshotId snapshotId = entry.getKey();
            snapshotInfos.add(new SnapshotInfo(snapshotId, indices, repositoryData.getSnapshotState(snapshotId)));
        }
        CollectionUtil.timSort(snapshotInfos);
        return Collections.unmodifiableList(snapshotInfos);
    }
}
