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

package io.crate.planner;

import java.util.UUID;
import java.util.function.BiFunction;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.UUIDs;
import org.jetbrains.annotations.Nullable;

import io.crate.session.Cursors;
import io.crate.analyze.WhereClause;
import io.crate.data.Row;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.tracer.LoggingOptimizerTracer;
import io.crate.planner.optimizer.tracer.OptimizerTracer;
import io.crate.protocols.postgres.TransactionState;

public class PlannerContext {

    public static PlannerContext forSubPlan(PlannerContext context) {
        return forSubPlan(context, context.fetchSize);
    }

    public static PlannerContext forSubPlan(PlannerContext context, int fetchSize) {
        return new PlannerContext(
            context.clusterState,
            context.routingProvider,
            new RoutingBuilder(context.clusterState, context.routingProvider),
            UUIDs.dirtyUUID(),
            context.coordinatorTxnCtx,
            context.nodeCtx,
            fetchSize,
            context.params,
            context.cursors,
            context.transactionState,
            context.planStats,
            context.optimizerTracer,
            context.optimize
        );
    }

    private final UUID jobId;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final Cursors cursors;
    private final int fetchSize;
    private final RoutingBuilder routingBuilder;
    private final RoutingProvider routingProvider;
    private final NodeContext nodeCtx;
    private final ClusterState clusterState;
    private final TransactionState transactionState;
    private int executionPhaseId = 0;
    private final String handlerNode;
    @Nullable
    private final Row params;
    private final PlanStats planStats;
    private final OptimizerTracer optimizerTracer;
    private final BiFunction<LogicalPlan, PlannerContext, LogicalPlan> optimize;

    /**
     * @param params See {@link #params()}
     */
    PlannerContext(ClusterState clusterState,
                   RoutingProvider routingProvider,
                   UUID jobId,
                   CoordinatorTxnCtx coordinatorTxnCtx,
                   NodeContext nodeCtx,
                   int fetchSize,
                   @Nullable Row params,
                   Cursors cursors,
                   TransactionState transactionState,
                   PlanStats planStats,
                   BiFunction<LogicalPlan, PlannerContext, LogicalPlan> optimize) {
        this(
            clusterState,
            routingProvider,
            new RoutingBuilder(clusterState, routingProvider),
            jobId,
            coordinatorTxnCtx,
            nodeCtx,
            fetchSize,
            params,
            cursors,
            transactionState,
            planStats,
            LoggingOptimizerTracer.getInstance(),
            optimize
        );
    }

    private PlannerContext(ClusterState clusterState,
                           RoutingProvider routingProvider,
                           RoutingBuilder routingBuilder,
                           UUID jobId,
                           CoordinatorTxnCtx coordinatorTxnCtx,
                           NodeContext nodeCtx,
                           int fetchSize,
                           @Nullable Row params,
                           Cursors cursors,
                           TransactionState transactionState,
                           PlanStats planStats,
                           OptimizerTracer optimizerTracer,
                           BiFunction<LogicalPlan, PlannerContext, LogicalPlan> optimize
                           ) {
        this.routingProvider = routingProvider;
        this.nodeCtx = nodeCtx;
        this.params = params;
        this.routingBuilder = routingBuilder;
        this.clusterState = clusterState;
        this.jobId = jobId;
        this.coordinatorTxnCtx = coordinatorTxnCtx;
        this.fetchSize = fetchSize;
        this.handlerNode = clusterState.nodes().getLocalNodeId();
        this.cursors = cursors;
        this.transactionState = transactionState;
        this.planStats = planStats;
        this.optimizerTracer = optimizerTracer;
        this.optimize = optimize;
    }

    public PlannerContext withOptimizerTracer(OptimizerTracer optimizerTracer) {
        return new PlannerContext(
            clusterState,
            routingProvider,
            routingBuilder,
            jobId,
            coordinatorTxnCtx,
            nodeCtx,
            fetchSize,
            params,
            cursors,
            transactionState,
            planStats,
            optimizerTracer,
            optimize
        );
    }

    /**
     * The parameters provided by the client for the param placeholders (`?`) in the query.
     * This can be `null` if the query is going to be executed as bulk-operation.
     */
    @Nullable
    public Row params() {
        return params;
    }

    public PlanStats planStats() {
        return planStats;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public CoordinatorTxnCtx transactionContext() {
        return coordinatorTxnCtx;
    }

    public String handlerNode() {
        return handlerNode;
    }

    public UUID jobId() {
        return jobId;
    }

    public int nextExecutionPhaseId() {
        return executionPhaseId++;
    }

    public Routing allocateRouting(TableInfo tableInfo,
                                   WhereClause where,
                                   RoutingProvider.ShardSelection shardSelection,
                                   CoordinatorSessionSettings sessionSettings) {
        return routingBuilder.allocateRouting(tableInfo, where, shardSelection, sessionSettings);
    }

    public ShardRouting resolveShard(String indexName, String id, @Nullable String routing) {
        return routingBuilder.resolveShard(indexName, id, routing);
    }

    public ReaderAllocations buildReaderAllocations() {
        return routingBuilder.buildReaderAllocations();
    }

    public NodeContext nodeContext() {
        return nodeCtx;
    }

    public ClusterState clusterState() {
        return clusterState;
    }

    public void newReaderAllocations() {
        routingBuilder.newAllocations();
    }

    public Cursors cursors() {
        return cursors;
    }

    public TransactionState transactionState() {
        return transactionState;
    }

    public OptimizerTracer optimizerTracer() {
        return optimizerTracer;
    }

    public BiFunction<LogicalPlan, PlannerContext, LogicalPlan> optimize() {
        return optimize;
    }
}
