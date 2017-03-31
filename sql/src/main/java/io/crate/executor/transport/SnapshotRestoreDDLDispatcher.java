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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.crate.action.FutureActionListener;
import io.crate.analyze.CreateSnapshotAnalyzedStatement;
import io.crate.analyze.DropSnapshotAnalyzedStatement;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.exceptions.CreateSnapshotException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.crate.analyze.SnapshotSettings.IGNORE_UNAVAILABLE;
import static io.crate.analyze.SnapshotSettings.WAIT_FOR_COMPLETION;


@Singleton
public class SnapshotRestoreDDLDispatcher {

    private static final ESLogger LOGGER = Loggers.getLogger(SnapshotRestoreDDLDispatcher.class);
    private final TransportActionProvider transportActionProvider;

    @Inject
    public SnapshotRestoreDDLDispatcher(TransportActionProvider transportActionProvider) {
        this.transportActionProvider = transportActionProvider;
    }

    public CompletableFuture<Long> dispatch(final DropSnapshotAnalyzedStatement statement) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        final String repositoryName = statement.repository();
        final String snapshotName = statement.snapshot();

        transportActionProvider.transportDeleteSnapshotAction().execute(
            new DeleteSnapshotRequest(repositoryName, snapshotName),
            new ActionListener<DeleteSnapshotResponse>() {
                @Override
                public void onResponse(DeleteSnapshotResponse response) {
                    if (!response.isAcknowledged()) {
                        LOGGER.info("delete snapshot '{}.{}' not acknowledged", repositoryName, snapshotName);
                    }
                    future.complete(1L);
                }

                @Override
                public void onFailure(Throwable e) {
                    future.completeExceptionally(e);
                }
            }
        );
        return future;

    }

    public CompletableFuture<Long> dispatch(final CreateSnapshotAnalyzedStatement statement) {
        final CompletableFuture<Long> resultFuture = new CompletableFuture<>();

        boolean waitForCompletion = statement.snapshotSettings().getAsBoolean(WAIT_FOR_COMPLETION.settingName(), WAIT_FOR_COMPLETION.defaultValue());
        boolean ignoreUnavailable = statement.snapshotSettings().getAsBoolean(IGNORE_UNAVAILABLE.settingName(), IGNORE_UNAVAILABLE.defaultValue());

        // ignore_unavailable as set by statement
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, IndicesOptions.lenientExpandOpen());

        CreateSnapshotRequest request = new CreateSnapshotRequest(statement.snapshotId().getRepository(), statement.snapshotId().getSnapshot())
            .includeGlobalState(statement.includeMetadata())
            .waitForCompletion(waitForCompletion)
            .indices(statement.indices())
            .indicesOptions(indicesOptions)
            .settings(statement.snapshotSettings());

        //noinspection ThrowableResultOfMethodCallIgnored
        assert request.validate() == null : "invalid CREATE SNAPSHOT statement";
        transportActionProvider.transportCreateSnapshotAction().execute(request, new ActionListener<CreateSnapshotResponse>() {
            @Override
            public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
                if (snapshotInfo == null) {
                    // if wait_for_completion is false the snapshotInfo is null
                    resultFuture.complete(1L);
                } else if (snapshotInfo.state() == SnapshotState.FAILED) {
                    // fail request if snapshot creation failed
                    String reason = createSnapshotResponse.getSnapshotInfo().reason()
                        .replaceAll("Index", "Table")
                        .replaceAll("Indices", "Tables");
                    resultFuture.completeExceptionally(
                        new CreateSnapshotException(statement.snapshotId(), reason)
                    );
                } else {
                    resultFuture.complete(1L);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                resultFuture.completeExceptionally(e);
            }
        });
        return resultFuture;
    }

    public CompletableFuture<Long> dispatch(final RestoreSnapshotAnalyzedStatement analysis) {
        boolean waitForCompletion = analysis.settings().getAsBoolean(WAIT_FOR_COMPLETION.settingName(), WAIT_FOR_COMPLETION.defaultValue());
        boolean ignoreUnavailable = analysis.settings().getAsBoolean(IGNORE_UNAVAILABLE.settingName(), IGNORE_UNAVAILABLE.defaultValue());

        // ignore_unavailable as set by statement
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, IndicesOptions.lenientExpandOpen());
        FutureActionListener<RestoreSnapshotResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        resolveIndexNames(analysis.restoreTables(), ignoreUnavailable, transportActionProvider.transportGetSnapshotsAction(), analysis.repositoryName())
            .whenComplete((List<String> indexNames, Throwable t) -> {
                if (t == null) {
                    RestoreSnapshotRequest request = new RestoreSnapshotRequest(analysis.repositoryName(), analysis.snapshotName())
                        .indices(indexNames)
                        .indicesOptions(indicesOptions)
                        .settings(analysis.settings())
                        .waitForCompletion(waitForCompletion)
                        .includeGlobalState(false)
                        .includeAliases(true);
                    transportActionProvider.transportRestoreSnapshotAction().execute(request, listener);
                } else {
                    listener.onFailure(t);
                }
            });
        return listener;
    }

    @VisibleForTesting
    static CompletableFuture<List<String>> resolveIndexNames(List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> restoreTables, boolean ignoreUnavailable,
                                                             TransportGetSnapshotsAction getSnapshotsAction, String repositoryName) {
        Collection<String> resolvedIndices = new HashSet<>();
        List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> toResolveFromSnapshot = new ArrayList<>();
        for (RestoreSnapshotAnalyzedStatement.RestoreTableInfo table : restoreTables) {
            if (table.hasPartitionInfo()) {
                resolvedIndices.add(table.partitionName().asIndexName());
            } else if (ignoreUnavailable) {
                // If ignoreUnavailable is true, it's cheaper to simply return indexName and the partitioned wildcard instead
                // checking if it's a partitioned table or not
                resolvedIndices.add(table.tableIdent().indexName());
                resolvedIndices.add(PartitionName.templateName(table.tableIdent().schema(), table.tableIdent().name()) + "*");
            } else {
                // index name needs to be resolved from snapshot
                toResolveFromSnapshot.add(table);
            }
        }
        if (toResolveFromSnapshot.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableList.copyOf(resolvedIndices));
        }
        final CompletableFuture<List<String>> f = new CompletableFuture<>();
        getSnapshotsAction.execute(
            new GetSnapshotsRequest(repositoryName),
            new ResolveFromSnapshotActionListener(f, toResolveFromSnapshot, resolvedIndices)
        );
        return f;
    }

    @VisibleForTesting
    static class ResolveFromSnapshotActionListener implements ActionListener<GetSnapshotsResponse> {

        private final CompletableFuture<List<String>> returnFuture;
        private final Collection<String> resolvedIndices;
        private final List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> toResolve;

        public ResolveFromSnapshotActionListener(CompletableFuture<List<String>> returnFuture,
                                                 List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> toResolve,
                                                 Collection<String> resolvedIndices) {
            this.returnFuture = returnFuture;
            this.resolvedIndices = resolvedIndices;
            this.toResolve = toResolve;
        }

        @Override
        public void onResponse(GetSnapshotsResponse getSnapshotsResponse) {
            List<SnapshotInfo> snapshots = getSnapshotsResponse.getSnapshots();
            for (RestoreSnapshotAnalyzedStatement.RestoreTableInfo table : toResolve) {
                try {
                    resolvedIndices.add(resolveIndexNameFromSnapshot(table.tableIdent(), snapshots));
                } catch (TableUnknownException e) {
                    returnFuture.completeExceptionally(e);
                }
            }
            returnFuture.complete(ImmutableList.copyOf(resolvedIndices));
        }

        @VisibleForTesting
        public static String resolveIndexNameFromSnapshot(TableIdent tableIdent, List<SnapshotInfo> snapshots) throws TableUnknownException{
            String indexName = tableIdent.indexName();
            for (SnapshotInfo snapshot : snapshots) {
                for (String index : snapshot.indices()) {
                    if (indexName.equals(index)) {
                        return indexName;
                    } else if(isIndexPartitionOfTable(index, tableIdent)) {
                        // add a partitions wildcard
                        // to match all partitions if a partitioned table was meant
                        return PartitionName.templateName(tableIdent.schema(), tableIdent.name()) + "*";
                    }
                }
            }
            throw new TableUnknownException(tableIdent);
        }

        private static boolean isIndexPartitionOfTable(String index, TableIdent tableIdent) {
            return PartitionName.isPartition(index) &&
                   PartitionName.fromIndexOrTemplate(index).tableIdent().equals(tableIdent);
        }

        @Override
        public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(e);
        }
    }
}
