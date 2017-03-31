/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.action.sql;

import io.crate.action.FutureActionListener;
import io.crate.analyze.*;
import io.crate.blob.v2.BlobAdminClient;
import io.crate.executor.transport.AlterTableOperation;
import io.crate.executor.transport.RepositoryService;
import io.crate.executor.transport.SnapshotRestoreDDLDispatcher;
import io.crate.executor.transport.TableCreator;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.admin.indices.upgrade.post.TransportUpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 * <p>
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
@Singleton
public class DDLStatementDispatcher {

    private final Provider<BlobAdminClient> blobAdminClient;
    private final TableCreator tableCreator;
    private final AlterTableOperation alterTableOperation;
    private final RepositoryService repositoryService;
    private final SnapshotRestoreDDLDispatcher snapshotRestoreDDLDispatcher;
    private final Provider<TransportUpgradeAction> transportUpgradeActionProvider;
    private final Provider<TransportForceMergeAction> transportForceMergeActionProvider;
    private final Provider<TransportRefreshAction> transportRefreshActionProvider;

    private final InnerVisitor innerVisitor = new InnerVisitor();


    @Inject
    public DDLStatementDispatcher(Provider<BlobAdminClient> blobAdminClient,
                                  TableCreator tableCreator,
                                  AlterTableOperation alterTableOperation,
                                  RepositoryService repositoryService,
                                  SnapshotRestoreDDLDispatcher snapshotRestoreDDLDispatcher,
                                  Provider<TransportUpgradeAction> transportUpgradeActionProvider,
                                  Provider<TransportForceMergeAction> transportForceMergeActionProvider,
                                  Provider<TransportRefreshAction> transportRefreshActionProvider) {
        this.blobAdminClient = blobAdminClient;
        this.tableCreator = tableCreator;
        this.alterTableOperation = alterTableOperation;
        this.repositoryService = repositoryService;
        this.snapshotRestoreDDLDispatcher = snapshotRestoreDDLDispatcher;
        this.transportUpgradeActionProvider = transportUpgradeActionProvider;
        this.transportForceMergeActionProvider = transportForceMergeActionProvider;
        this.transportRefreshActionProvider = transportRefreshActionProvider;
    }

    public CompletableFuture<Long> dispatch(AnalyzedStatement analyzedStatement, UUID jobId) {
        return innerVisitor.process(analyzedStatement, jobId);
    }

    private class InnerVisitor extends AnalyzedStatementVisitor<UUID, CompletableFuture<Long>> {

        @Override
        protected CompletableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, UUID jobId) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement));
        }


        @Override
        public CompletableFuture<Long> visitCreateTableStatement(CreateTableAnalyzedStatement analysis, UUID jobId) {
            return tableCreator.create(analysis);
        }

        @Override
        public CompletableFuture<Long> visitAlterTableStatement(final AlterTableAnalyzedStatement analysis, UUID jobId) {
            return alterTableOperation.executeAlterTable(analysis);
        }

        @Override
        public CompletableFuture<Long> visitAddColumnStatement(AddColumnAnalyzedStatement analysis, UUID context) {
            return alterTableOperation.executeAlterTableAddColumn(analysis);
        }

        @Override
        public CompletableFuture<Long> visitOptimizeTableStatement(OptimizeTableAnalyzedStatement analysis, UUID jobId) {
            if (analysis.settings().getAsBoolean(OptimizeSettings.UPGRADE_SEGMENTS.name(),
                OptimizeSettings.UPGRADE_SEGMENTS.defaultValue())) {
                return executeUpgradeSegments(analysis, transportUpgradeActionProvider.get());
            } else {
                return executeMergeSegments(analysis, transportForceMergeActionProvider.get());
            }
        }

        @Override
        public CompletableFuture<Long> visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, UUID jobId) {
            if (analysis.indexNames().isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            RefreshRequest request = new RefreshRequest(analysis.indexNames().toArray(
                new String[analysis.indexNames().size()]));
            request.indicesOptions(IndicesOptions.lenientExpandOpen());

            FutureActionListener<RefreshResponse, Long> listener =
                new FutureActionListener<>(r -> (long) analysis.indexNames().size());
            transportRefreshActionProvider.get().execute(request, listener);
            return listener;
        }


        @Override
        public CompletableFuture<Long> visitCreateBlobTableStatement(CreateBlobTableAnalyzedStatement analysis,
                                                                     UUID jobId) {
            return blobAdminClient.get().createBlobTable( analysis.tableName(), analysis.tableParameter().settings());
        }

        @Override
        public CompletableFuture<Long> visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, UUID jobId) {
            return blobAdminClient.get()
                .alterBlobTable(analysis.table().ident().name(), analysis.tableParameter().settings());
        }

        @Override
        public CompletableFuture<Long> visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, UUID jobId) {
            return blobAdminClient.get().dropBlobTable(analysis.table().ident().name());
        }

        @Override
        public CompletableFuture<Long> visitDropRepositoryAnalyzedStatement(DropRepositoryAnalyzedStatement analysis, UUID jobId) {
            return repositoryService.execute(analysis);
        }

        @Override
        public CompletableFuture<Long> visitCreateRepositoryAnalyzedStatement(CreateRepositoryAnalyzedStatement analysis, UUID jobId) {
            return repositoryService.execute(analysis);
        }

        @Override
        public CompletableFuture<Long> visitDropSnapshotAnalyzedStatement(DropSnapshotAnalyzedStatement analysis, UUID jobId) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        public CompletableFuture<Long> visitCreateSnapshotAnalyzedStatement(CreateSnapshotAnalyzedStatement analysis, UUID jobId) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        @Override
        public CompletableFuture<Long> visitRestoreSnapshotAnalyzedStatement(RestoreSnapshotAnalyzedStatement analysis, UUID context) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }
    }

    private static CompletableFuture<Long> executeMergeSegments(OptimizeTableAnalyzedStatement analysis,
                                                                TransportForceMergeAction transportForceMergeAction) {
        ForceMergeRequest request = new ForceMergeRequest(analysis.indexNames().toArray(new String[0]));

        // Pass parameters to ES request
        request.maxNumSegments(analysis.settings().getAsInt(OptimizeSettings.MAX_NUM_SEGMENTS.name(),
            ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS));
        request.onlyExpungeDeletes(analysis.settings().getAsBoolean(OptimizeSettings.ONLY_EXPUNGE_DELETES.name(),
            ForceMergeRequest.Defaults.ONLY_EXPUNGE_DELETES));
        request.flush(analysis.settings().getAsBoolean(OptimizeSettings.FLUSH.name(),
            ForceMergeRequest.Defaults.FLUSH));

        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        FutureActionListener<ForceMergeResponse, Long> listener =
            new FutureActionListener<>(r -> (long) analysis.indexNames().size());
        transportForceMergeAction.execute(request, listener);
        return listener;
    }

    private static CompletableFuture<Long> executeUpgradeSegments(OptimizeTableAnalyzedStatement analysis,
                                                                  TransportUpgradeAction transportUpgradeAction) {
        UpgradeRequest request = new UpgradeRequest(analysis.indexNames().toArray(new String[0]));
        FutureActionListener<UpgradeResponse, Long> listener =
            new FutureActionListener<>(r -> (long) analysis.indexNames().size());
        transportUpgradeAction.execute(request, listener);
        return listener;
    }
}
