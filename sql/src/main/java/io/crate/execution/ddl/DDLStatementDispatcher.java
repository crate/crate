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

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.PromoteReplicaStatement;
import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteCancelShardAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.RerouteRetryFailedAnalyzedStatement;
import io.crate.analyze.RestoreSnapshotAnalyzedStatement;
import io.crate.data.Row;
import io.crate.execution.support.Transports;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.failedFuture;

/**
 * visitor that dispatches requests based on Analysis class to different actions.
 * <p>
 * Its methods return a future returning a Long containing the response rowCount.
 * If the future returns <code>null</code>, no row count shall be created.
 */
@Singleton
public class DDLStatementDispatcher {

    private ClusterService clusterService;
    private final SnapshotRestoreDDLDispatcher snapshotRestoreDDLDispatcher;

    private final InnerVisitor innerVisitor = new InnerVisitor();
    private final TransportClusterRerouteAction rerouteAction;
    private final Functions functions;


    @Inject
    public DDLStatementDispatcher(ClusterService clusterService,
                                  SnapshotRestoreDDLDispatcher snapshotRestoreDDLDispatcher,
                                  TransportClusterRerouteAction rerouteAction,
                                  Functions functions) {
        this.clusterService = clusterService;
        this.snapshotRestoreDDLDispatcher = snapshotRestoreDDLDispatcher;
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

        @Override
        public CompletableFuture<Long> visitRestoreSnapshotAnalyzedStatement(RestoreSnapshotAnalyzedStatement analysis,
                                                                             Ctx ctx) {
            return snapshotRestoreDDLDispatcher.dispatch(analysis);
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
}
