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
import io.crate.action.FutureActionListener;
import io.crate.analyze.CreateSnapshotAnalyzedStatement;
import io.crate.analyze.DropSnapshotAnalyzedStatement;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.exceptions.CreateSnapshotException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.apache.logging.log4j.Logger;
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

    private static final Logger LOGGER = Loggers.getLogger(SnapshotRestoreDDLDispatcher.class);
    private final TransportActionProvider transportActionProvider;
    private final String[] ALL_TEMPLATES = new String[]{"_all"};

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
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        return future;

    }

    public CompletableFuture<Long> dispatch(final CreateSnapshotAnalyzedStatement statement) {
        final CompletableFuture<Long> resultFuture = new CompletableFuture<>();

        boolean waitForCompletion = statement.snapshotSettings().getAsBoolean(WAIT_FOR_COMPLETION.name(), WAIT_FOR_COMPLETION.defaultValue());
        boolean ignoreUnavailable = statement.snapshotSettings().getAsBoolean(IGNORE_UNAVAILABLE.name(), IGNORE_UNAVAILABLE.defaultValue());

        // ignore_unavailable as set by statement
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, IndicesOptions.lenientExpandOpen());

        CreateSnapshotRequest request = new CreateSnapshotRequest(statement.snapshot().getRepository(), statement.snapshot().getSnapshotId().getName())
            .includeGlobalState(true)
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
                        new CreateSnapshotException(statement.snapshot(), reason)
                    );
                } else {
                    resultFuture.complete(1L);
                }
            }

            @Override
            public void onFailure(Exception e) {
                resultFuture.completeExceptionally(e);
            }
        });
        return resultFuture;
    }

    public CompletableFuture<Long> dispatch(final RestoreSnapshotAnalyzedStatement analysis) {
        boolean waitForCompletion = analysis.settings().getAsBoolean(WAIT_FOR_COMPLETION.name(), WAIT_FOR_COMPLETION.defaultValue());
        boolean ignoreUnavailable = analysis.settings().getAsBoolean(IGNORE_UNAVAILABLE.name(), IGNORE_UNAVAILABLE.defaultValue());

        // ignore_unavailable as set by statement
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, true, true, false, IndicesOptions.lenientExpandOpen());
        FutureActionListener<RestoreSnapshotResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        resolveIndexNames(analysis.restoreTables(), ignoreUnavailable, transportActionProvider.transportGetSnapshotsAction(), analysis.repositoryName())
            .whenComplete((ResolveIndicesAndTemplatesContext ctx, Throwable t) -> {
                if (t == null) {
                    String[] indexNames = ctx.resolvedIndices().toArray(new String[ctx.resolvedIndices().size()]);
                    String[] templateNames = analysis.restoreAll() ? ALL_TEMPLATES :
                        ctx.resolvedTemplates().toArray(new String[ctx.resolvedTemplates().size()]);
                    RestoreSnapshotRequest request = new RestoreSnapshotRequest(analysis.repositoryName(), analysis.snapshotName())
                        .indices(indexNames)
                        .templates(templateNames)
                        .indicesOptions(indicesOptions)
                        .settings(analysis.settings())
                        .waitForCompletion(waitForCompletion)
                        .includeGlobalState(false)
                        .includeAliases(true);
                    transportActionProvider.transportRestoreSnapshotAction().execute(request, listener);
                } else {
                    listener.onFailure((Exception) t);
                }
            });
        return listener;
    }

    @VisibleForTesting
    static CompletableFuture<ResolveIndicesAndTemplatesContext> resolveIndexNames(List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> restoreTables, boolean ignoreUnavailable,
                                                                                  TransportGetSnapshotsAction getSnapshotsAction, String repositoryName) {
        ResolveIndicesAndTemplatesContext resolveIndicesAndTemplatesContext = new ResolveIndicesAndTemplatesContext();
        List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> toResolveFromSnapshot = new ArrayList<>();
        for (RestoreSnapshotAnalyzedStatement.RestoreTableInfo table : restoreTables) {
            if (table.hasPartitionInfo()) {
                resolveIndicesAndTemplatesContext.addIndex(table.partitionName().asIndexName());
                resolveIndicesAndTemplatesContext.addTemplate(table.partitionTemplate());
            } else if (ignoreUnavailable) {
                // If ignoreUnavailable is true, it's cheaper to simply return indexName and the partitioned wildcard instead
                // checking if it's a partitioned table or not
                resolveIndicesAndTemplatesContext.addIndex(table.tableIdent().indexName());
                // For the case its a partitioned table we restore all partitions and the templates
                String templateName = table.partitionTemplate();
                resolveIndicesAndTemplatesContext.addIndex(templateName + "*");
                resolveIndicesAndTemplatesContext.addTemplate(templateName);
            } else {
                // index name needs to be resolved from snapshot
                toResolveFromSnapshot.add(table);
            }
        }
        if (toResolveFromSnapshot.isEmpty()) {
            return CompletableFuture.completedFuture(resolveIndicesAndTemplatesContext);
        }
        final CompletableFuture<ResolveIndicesAndTemplatesContext> f = new CompletableFuture<>();
        getSnapshotsAction.execute(
            new GetSnapshotsRequest(repositoryName),
            new ResolveFromSnapshotActionListener(f, toResolveFromSnapshot, resolveIndicesAndTemplatesContext)
        );
        return f;
    }

    @VisibleForTesting
    static class ResolveIndicesAndTemplatesContext {

        private final Collection<String> resolvedIndices = new HashSet<>();
        private final Collection<String> resolvedTemplates = new HashSet<>();

        void addIndex(String index) {
            resolvedIndices.add(index);
        }

        void addTemplate(String template) {
            resolvedTemplates.add(template);
        }

        Collection<String> resolvedIndices() {
            return resolvedIndices;
        }

        Collection<String> resolvedTemplates() {
            return resolvedTemplates;
        }
    }

    @VisibleForTesting
    static class ResolveFromSnapshotActionListener implements ActionListener<GetSnapshotsResponse> {

        private final CompletableFuture<ResolveIndicesAndTemplatesContext> returnFuture;
        private final ResolveIndicesAndTemplatesContext resolveIndicesAndTemplatesContext;
        private final List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> toResolve;

        public ResolveFromSnapshotActionListener(CompletableFuture<ResolveIndicesAndTemplatesContext> returnFuture,
                                                 List<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> toResolve,
                                                 ResolveIndicesAndTemplatesContext resolveIndicesAndTemplatesContext) {
            this.returnFuture = returnFuture;
            this.resolveIndicesAndTemplatesContext = resolveIndicesAndTemplatesContext;
            this.toResolve = toResolve;
        }

        @Override
        public void onResponse(GetSnapshotsResponse getSnapshotsResponse) {
            List<SnapshotInfo> snapshots = getSnapshotsResponse.getSnapshots();
            for (RestoreSnapshotAnalyzedStatement.RestoreTableInfo table : toResolve) {
                resolveTableFromSnapshot(table, snapshots, resolveIndicesAndTemplatesContext);
            }
            returnFuture.complete(resolveIndicesAndTemplatesContext);
        }

        @VisibleForTesting
        public static void resolveTableFromSnapshot(RestoreSnapshotAnalyzedStatement.RestoreTableInfo table,
                                                    List<SnapshotInfo> snapshots,
                                                    ResolveIndicesAndTemplatesContext ctx) throws TableUnknownException {
            String name = table.tableIdent().indexName();
            for (SnapshotInfo snapshot : snapshots) {
                for (String index : snapshot.indices()) {
                    if (name.equals(index)) {
                        ctx.addIndex(index);
                        return;
                    } else if(isIndexPartitionOfTable(index, table.tableIdent())) {
                        String templateName = table.partitionTemplate();
                        // add a partitions wildcard
                        // to match all partitions if a partitioned table was meant
                        ctx.addIndex(templateName + "*");
                        ctx.addTemplate(templateName);
                        return;
                    }
                }
            }
            ctx.addTemplate(table.partitionTemplate());
        }

        private static boolean isIndexPartitionOfTable(String index, TableIdent tableIdent) {
            return IndexParts.isPartitioned(index) &&
                   PartitionName.fromIndexOrTemplate(index).tableIdent().equals(tableIdent);
        }

        @Override
        public void onFailure(Exception e) {
            returnFuture.completeExceptionally(e);
        }
    }
}
