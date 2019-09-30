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
import io.crate.analyze.AlterBlobTableAnalyzedStatement;
import io.crate.analyze.AlterTableOpenCloseAnalyzedStatement;
import io.crate.analyze.AlterTableRenameAnalyzedStatement;
import io.crate.analyze.AlterUserAnalyzedStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.CreateBlobTableAnalyzedStatement;
import io.crate.analyze.CreateFunctionAnalyzedStatement;
import io.crate.analyze.CreateRepositoryAnalyzedStatement;
import io.crate.analyze.CreateSnapshotAnalyzedStatement;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropFunctionAnalyzedStatement;
import io.crate.analyze.DropRepositoryAnalyzedStatement;
import io.crate.analyze.DropSnapshotAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.analyze.OptimizeTableAnalyzedStatement;
import io.crate.analyze.PromoteReplicaStatement;
import io.crate.analyze.RefreshTableAnalyzedStatement;
import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteCancelShardAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.RerouteRetryFailedAnalyzedStatement;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.auth.user.UserManager;
import io.crate.blob.v2.BlobAdminClient;
import io.crate.data.Row;
import io.crate.execution.ddl.tables.AlterTableOperation;
import io.crate.execution.support.Transports;
import io.crate.expression.udf.UserDefinedFunctionDDLClient;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.user.SecureHash;
import io.crate.user.UserActions;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import static io.crate.analyze.OptimizeTableAnalyzer.FLUSH;
import static io.crate.analyze.OptimizeTableAnalyzer.MAX_NUM_SEGMENTS;
import static io.crate.analyze.OptimizeTableAnalyzer.ONLY_EXPUNGE_DELETES;
import static io.crate.analyze.OptimizeTableAnalyzer.UPGRADE_SEGMENTS;
import static java.util.concurrent.CompletableFuture.failedFuture;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 * <p>
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
@Singleton
public class DDLStatementDispatcher {

    private final Provider<BlobAdminClient> blobAdminClient;
    private ClusterService clusterService;
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
                                  ClusterService clusterService,
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
        this.clusterService = clusterService;
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

    public CompletableFuture<Long> apply(AnalyzedStatement analyzedStatement, Row parameters, TransactionContext txnCtx) {
        try {
            return analyzedStatement.accept(innerVisitor, new Ctx(txnCtx, parameters));
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    private static class Ctx {
        final TransactionContext txnCtx;
        final Row parameters;

        Ctx(TransactionContext txnCtx, Row parameters) {
            this.txnCtx = txnCtx;
            this.parameters = parameters;
        }
    }

    private class InnerVisitor extends AnalyzedStatementVisitor<Ctx, CompletableFuture<Long>> {

        @Override
        protected CompletableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Ctx ctx) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement));
        }

        public CompletableFuture<Long> visitAlterTableOpenCloseStatement(AlterTableOpenCloseAnalyzedStatement analysis,
                                                                         Ctx ctx) {
            return alterTableOperation.executeAlterTableOpenClose(analysis);
        }

        @Override
        public CompletableFuture<Long> visitAlterTableRenameStatement(AlterTableRenameAnalyzedStatement analysis, Ctx ctx) {
            return alterTableOperation.executeAlterTableRenameTable(analysis);
        }

        @Override
        public CompletableFuture<Long> visitOptimizeTableStatement(OptimizeTableAnalyzedStatement analysis, Ctx ctx) {
            if (UPGRADE_SEGMENTS.get(analysis.settings())) {
                return executeUpgradeSegments(analysis, transportUpgradeActionProvider.get());
            } else {
                return executeMergeSegments(analysis, transportForceMergeActionProvider.get());
            }
        }

        @Override
        public CompletableFuture<Long> visitRefreshTableStatement(RefreshTableAnalyzedStatement analysis, Ctx ctx) {
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
                                                                     Ctx ctx) {
            return blobAdminClient.get().createBlobTable(analysis.tableName(), analysis.tableParameter().settings());
        }

        @Override
        public CompletableFuture<Long> visitAlterBlobTableStatement(AlterBlobTableAnalyzedStatement analysis, Ctx ctx) {
            return blobAdminClient.get().alterBlobTable(analysis.table().ident().name(), analysis.tableParameter().settings());
        }

        @Override
        public CompletableFuture<Long> visitDropRepositoryAnalyzedStatement(DropRepositoryAnalyzedStatement analysis,
                                                                            Ctx ctx) {
            return repositoryService.execute(analysis);
        }

        @Override
        public CompletableFuture<Long> visitCreateRepositoryAnalyzedStatement(CreateRepositoryAnalyzedStatement analysis,
                                                                              Ctx ctx) {
            return repositoryService.execute(analysis);
        }

        @Override
        public CompletableFuture<Long> visitDropSnapshotAnalyzedStatement(DropSnapshotAnalyzedStatement analysis,
                                                                          Ctx ctx) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        public CompletableFuture<Long> visitCreateSnapshotAnalyzedStatement(CreateSnapshotAnalyzedStatement analysis,
                                                                            Ctx ctx) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        @Override
        public CompletableFuture<Long> visitRestoreSnapshotAnalyzedStatement(RestoreSnapshotAnalyzedStatement analysis,
                                                                             Ctx ctx) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
        }

        @Override
        protected CompletableFuture<Long> visitCreateFunctionStatement(CreateFunctionAnalyzedStatement analysis,
                                                                       Ctx ctx) {
            return udfDDLClient.execute(analysis, ctx.parameters);
        }

        @Override
        public CompletableFuture<Long> visitDropFunctionStatement(DropFunctionAnalyzedStatement analysis, Ctx ctx) {
            return udfDDLClient.execute(analysis);
        }

        @Override
        protected CompletableFuture<Long> visitCreateUserStatement(CreateUserAnalyzedStatement analysis, Ctx ctx) {
            SecureHash secureHash;
            try {
                secureHash = UserActions.generateSecureHash(analysis.properties(), ctx.parameters, ctx.txnCtx, functions);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                return failedFuture(e);
            }
            return userManager.createUser(analysis.userName(), secureHash);
        }

        @Override
        public CompletableFuture<Long> visitAlterUserStatement(AlterUserAnalyzedStatement analysis, Ctx ctx) {
            SecureHash newPassword;
            try {
                newPassword = UserActions.generateSecureHash(analysis.properties(), ctx.parameters, ctx.txnCtx, functions);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                return failedFuture(e);
            }
            return userManager.alterUser(analysis.userName(), newPassword);
        }

        @Override
        protected CompletableFuture<Long> visitDropUserStatement(DropUserAnalyzedStatement analysis, Ctx ctx) {
            return userManager.dropUser(analysis.userName(), analysis.ifExists());
        }

        @Override
        public CompletableFuture<Long> visitReroutePromoteReplica(PromoteReplicaStatement promoteReplica, Ctx ctx) {
            return Transports.execute(
                rerouteAction,
                RerouteActions.preparePromoteReplicaReq(
                    promoteReplica,
                    functions,
                    ctx.parameters,
                    ctx.txnCtx,
                    clusterService.state().nodes()),
                Transports.rowCountFromAcknowledgedResp()
            );
        }

        @Override
        protected CompletableFuture<Long> visitRerouteMoveShard(RerouteMoveShardAnalyzedStatement stmt, Ctx ctx) {
            return Transports.execute(
                rerouteAction,
                RerouteActions.prepareMoveShardReq(stmt, ctx.parameters, clusterService.state().nodes()),
                Transports.rowCountFromAcknowledgedResp()
            );
        }

        @Override
        protected CompletableFuture<Long> visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShardAnalyzedStatement stmt, Ctx ctx) {
            return Transports.execute(
                rerouteAction,
                RerouteActions.prepareAllocateReplicaReq(stmt, ctx.parameters, clusterService.state().nodes()),
                Transports.rowCountFromAcknowledgedResp()
            );
        }

        @Override
        protected CompletableFuture<Long> visitRerouteCancelShard(RerouteCancelShardAnalyzedStatement stmt, Ctx ctx) {
            return Transports.execute(
                rerouteAction,
                RerouteActions.prepareCancelShardReq(stmt, ctx.parameters, clusterService.state().nodes()),
                Transports.rowCountFromAcknowledgedResp()
            );
        }

        @Override
        public CompletableFuture<Long> visitRerouteRetryFailedStatement(RerouteRetryFailedAnalyzedStatement analysis, Ctx ctx) {
            return Transports.execute(
                rerouteAction,
                new ClusterRerouteRequest().setRetryFailed(true),
                RerouteActions::retryFailedAffectedShardsRowCount
            );
        }

    }

    private static CompletableFuture<Long> executeMergeSegments(OptimizeTableAnalyzedStatement analysis,
                                                                TransportForceMergeAction transportForceMergeAction) {
        ForceMergeRequest request = new ForceMergeRequest(analysis.indexNames().toArray(new String[0]));

        // Pass parameters to ES request
        Settings settings = analysis.settings();
        request.maxNumSegments(MAX_NUM_SEGMENTS.get(settings));
        request.onlyExpungeDeletes(ONLY_EXPUNGE_DELETES.get(settings));
        request.flush(FLUSH.get(settings));

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
