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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.data.Row;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;

import javax.annotation.Nullable;
import java.util.UUID;

public class PlannerContext {

    public static PlannerContext forSubPlan(PlannerContext context) {
        return forSubPlan(context, context.fetchSize);
    }

    public static PlannerContext forSubPlan(PlannerContext context, int fetchSize) {
        return new PlannerContext(
            context.clusterState,
            context.routingProvider,
            UUID.randomUUID(),
            context.coordinatorTxnCtx,
            context.nodeCtx,
            fetchSize,
            context.params
        );
    }

    private final UUID jobId;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final int fetchSize;
    private final RoutingBuilder routingBuilder;
    private final RoutingProvider routingProvider;
    private final NodeContext nodeCtx;
    private final ClusterState clusterState;
    private int executionPhaseId = 0;
    private final String handlerNode;
    @Nullable
    private final Row params;

    /**
     * @param params See {@link #params()}
     */
    public PlannerContext(ClusterState clusterState,
                          RoutingProvider routingProvider,
                          UUID jobId,
                          CoordinatorTxnCtx coordinatorTxnCtx,
                          NodeContext nodeCtx,
                          int fetchSize,
                          @Nullable Row params) {
        this.routingProvider = routingProvider;
        this.nodeCtx = nodeCtx;
        this.params = params;
        this.routingBuilder = new RoutingBuilder(clusterState, routingProvider);
        this.clusterState = clusterState;
        this.jobId = jobId;
        this.coordinatorTxnCtx = coordinatorTxnCtx;
        this.fetchSize = fetchSize;
        this.handlerNode = clusterState.getNodes().getLocalNodeId();
    }

    /**
     * The parameters provided by the client for the param placeholders (`?`) in the query.
     * This can be `null` if the query is going to be executed as bulk-operation.
     */
    @Nullable
    public Row params() {
        return params;
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
                                   SessionContext sessionContext) {
        return routingBuilder.allocateRouting(tableInfo, where, shardSelection, sessionContext);
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
}
