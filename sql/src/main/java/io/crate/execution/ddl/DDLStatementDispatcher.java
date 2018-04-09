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

import io.crate.action.FutureActionListener;
import io.crate.analyze.AddColumnAnalyzedStatement;
import io.crate.analyze.AlterBlobTableAnalyzedStatement;
import io.crate.analyze.AlterTableAnalyzedStatement;
import io.crate.analyze.AlterTableOpenCloseAnalyzedStatement;
import io.crate.analyze.AlterTableRenameAnalyzedStatement;
import io.crate.analyze.AlterUserAnalyzedStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.CreateBlobTableAnalyzedStatement;
import io.crate.analyze.CreateFunctionAnalyzedStatement;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.analyze.CreateSnapshotAnalyzedStatement;
import io.crate.analyze.CreateTableAnalyzedStatement;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropBlobTableAnalyzedStatement;
import io.crate.analyze.DropFunctionAnalyzedStatement;
import io.crate.analyze.DropRepositoryAnalyzedStatement;
import io.crate.analyze.DropSnapshotAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.analyze.OptimizeSettings;
import io.crate.analyze.OptimizeTableAnalyzedStatement;
import io.crate.analyze.RefreshTableAnalyzedStatement;
import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteCancelShardAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.RerouteRetryFailedAnalyzedStatement;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.blob.v2.BlobAdminClient;
import io.crate.data.Row;
import io.crate.execution.ddl.tables.AlterTableOperation;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.metadata.Functions;
import io.crate.expression.udf.UserDefinedFunctionDDLClient;
import io.crate.auth.user.UserManager;
import io.crate.user.SecureHash;
import io.crate.user.UserActions;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
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

import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static io.crate.concurrent.CompletableFutures.failedFuture;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 * <p>
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
@Singleton
public class DDLStatementDispatcher implements BiFunction<AnalyzedStatement, Row, CompletableFuture<Long>> {

    private final Provider<BlobAdminClient> blobAdminClient;
    private final TableCreator tableCreator;
    private final AlterTableOperation alterTableOperation;
    private final RepositoryService repositoryService;
    private final SnapshotRestoreDDLDispatcher snapshotRestoreDDLDispatcher;
    private final UserDefinedFunctionDDLClient udfDDLClient;
    private final Provider<TransportUpgradeAction> transportUpgradeActionProvider;
    private final Provider<TransportForceMergeAction> transportForceMergeActionProvider;
    private final Provider<TransportRefreshAction> transportRefreshActionProvider;
    private final UserManager userManager;

    private final InnerVisitor innerVisitor = new InnerVisitor();
    private final TransportClusterRerouteAction rerouteAction;
    private final Functions functions;


    @Inject
    public DDLStatementDispatcher(Provider<BlobAdminClient> blobAdminClient,
                                  TableCreator tableCreator,
                                  AlterTableOperation alterTableOperation,
                                  RepositoryService repositoryService,
                                  SnapshotRestoreDDLDispatcher snapshotRestoreDDLDispatcher,
                                  UserDefinedFunctionDDLClient udfDDLClient,
                                  TransportClusterRerouteAction rerouteAction,
                                  Provider<UserManager> userManagerProvider,
                                  Provider<TransportUpgradeAction> transportUpgradeActionProvider,
                                  Provider<TransportForceMergeAction> transportForceMergeActionProvider,
                                  Provider<TransportRefreshAction> transportRefreshActionProvider,
                                  Functions functions) {
        this.blobAdminClient = blobAdminClient;
        this.tableCreator = tableCreator;
        this.alterTableOperation = alterTableOperation;
        this.repositoryService = repositoryService;
        this.snapshotRestoreDDLDispatcher = snapshotRestoreDDLDispatcher;
        this.udfDDLClient = udfDDLClient;
        this.transportUpgradeActionProvider = transportUpgradeActionProvider;
        this.transportForceMergeActionProvider = transportForceMergeActionProvider;
        this.transportRefreshActionProvider = transportRefreshActionProvider;
        this.userManager = userManagerProvider.get();
        this.rerouteAction = rerouteAction;
        this.functions = functions;
    }

    @Override
    public CompletableFuture<Long> apply(AnalyzedStatement analyzedStatement, Row parameters) {
        return innerVisitor.process(analyzedStatement, parameters);
    }

    private class InnerVisitor extends AnalyzedStatementVisitor<Row, CompletableFuture<Long>> {

        @Override
        protected CompletableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Row parameters) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement));
        }

        @Override
        public CompletableFuture<Long> visitCreateTableStatement(CreateTableAnalyzedStatement analysis, Row parameters) {
            return tableCreator.create(analysis);
        }

        @Override
        public CompletableFuture<Long> visitAlterTableStatement(final AlterTableAnalyzedStatement analysis, Row parameters) {
            return alterTableOperation.executeAlterTable(analysis);
        }

        @Override
        public CompletableFuture<Long> visitAddColumnStatement(AddColumnAnalyzedStatement analysis, Row parameters) {
            return alterTableOperation.executeAlterTableAddColumn(analysis);
        }

        public CompletableFuture<Long> visitAlterTableOpenCloseStatement(AlterTableOpenCloseAnalyzedStatement analysis,
                                                                         Row parameters) {
            return alterTableOperation.executeAlterTableOpenClose(analysis);
        }

        @Override
        public CompletableFuture<Long> visitAlterTableRenameStatement(AlterTableRenameAnalyzedStatement analysis, Row parameters) {
            return alterTableOperation.executeAlterTableRenameTable(analysis);
        }

        @Override
        public CompletableFuture<Long> visitRerouteRetryFailedStatement(RerouteRetryFailedAnalyzedStatement analysis, Row parameters) {
            return RerouteActions.executeRetryFailed(rerouteAction::execute);
        }

        @Override
        public CompletableFuture<Long> visitOptimizeTableStatement(OptimizeTableAnalyzedStatement analysis, Row parameters) {
            if (analysis.settings().getAsBoolean(OptimizeSettings.UPGRADE_SEGMENTS.name(),
                OptimizeSettings.UPGRADE_SEGMENTS.defaultValue())) {
                return executeUpgradeSegments(analysis, transportUpgradeActionProvider.get());
            } else {
                return executeMergeSegments(analysis, transportForceMergeActionProvider.get());
            }
        }

        @Override
        public CompletableFuture<Long> visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, Row parameters) {
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
                                                                     Row parameters) {
            return blobAdminClient.get().createBlobTable(analysis.tableName(), analysis.tableParameter().settings());
        }

        @Override
        public CompletableFuture<Long> visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, Row parameters) {
            return blobAdminClient.get().alterBlobTable(analysis.table().ident().name(), analysis.tableParameter().settings());
        }

        @Override
        public CompletableFuture<Long> visitDropBlobTableStatement(DropBlobTableAnalyzedStatement analysis, Row parameters) {
            return blobAdminClient.get().dropBlobTable(analysis.table().ident().name());
        }

        @Override
        public CompletableFuture<Long> visitDropRepositoryAnalyzedStatement(DropRepositoryAnalyzedStatement analysis,
                                                                            Row parameters) {
            return repositoryService.execute(analysis);
        }

        @Override
        public CompletableFuture<Long> visitCreateRepositoryAnalyzedStatement(CreateRepositoryAnalyzedStatement analysis,
                                                                              Row parameters) {
            return repositoryService.execute(analysis);
        }

        @Override
        public CompletableFuture<Long> visitDropSnapshotAnalyzedStatement(DropSnapshotAnalyzedStatement analysis,
                                                                          Row parameters) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        public CompletableFuture<Long> visitCreateSnapshotAnalyzedStatement(CreateSnapshotAnalyzedStatement analysis,
                                                                            Row parameters) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        @Override
        public CompletableFuture<Long> visitRestoreSnapshotAnalyzedStatement(RestoreSnapshotAnalyzedStatement analysis,
                                                                             Row parameters) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        @Override
        protected CompletableFuture<Long> visitCreateFunctionStatement(CreateFunctionAnalyzedStatement analysis,
                                                                       Row parameters) {
            return udfDDLClient.execute(analysis, parameters);
        }

        @Override
        public CompletableFuture<Long> visitDropFunctionStatement(DropFunctionAnalyzedStatement analysis, Row parameters) {
            return udfDDLClient.execute(analysis);
        }

        @Override
        protected CompletableFuture<Long> visitCreateUserStatement(CreateUserAnalyzedStatement analysis, Row parameters) {
            SecureHash secureHash;
            try {
                secureHash = UserActions.generateSecureHash(analysis.properties(), parameters, functions);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                return failedFuture(e);
            }
            return userManager.createUser(analysis.userName(), secureHash);
        }

        @Override
        public CompletableFuture<Long> visitAlterUserStatement(AlterUserAnalyzedStatement analysis, Row parameters) {
            SecureHash newPassword;
            try {
                newPassword = UserActions.generateSecureHash(analysis.properties(), parameters, functions);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                return failedFuture(e);
            }
            return userManager.alterUser(analysis.userName(), newPassword);
        }

        @Override
        protected CompletableFuture<Long> visitDropUserStatement(DropUserAnalyzedStatement analysis, Row parameters) {
            return userManager.dropUser(analysis.userName(), analysis.ifExists());
        }

        @Override
        protected CompletableFuture<Long> visitRerouteMoveShard(RerouteMoveShardAnalyzedStatement analysis, Row parameters) {
            return RerouteActions.execute(rerouteAction::execute, analysis, parameters);
        }

        @Override
        protected CompletableFuture<Long> visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShardAnalyzedStatement analysis, Row parameters) {
            return RerouteActions.execute(rerouteAction::execute, analysis, parameters);
        }

        @Override
        protected CompletableFuture<Long> visitRerouteCancelShard(RerouteCancelShardAnalyzedStatement analysis, Row parameters) {
            return RerouteActions.execute(rerouteAction::execute, analysis, parameters);
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
