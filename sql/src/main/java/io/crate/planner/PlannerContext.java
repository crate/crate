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

package io.crate.planner;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import javax.annotation.Nullable;
import java.util.UUID;

public class PlannerContext {

    public static PlannerContext forSubPlan(PlannerContext context) {
        return forSubPlan(context, context.softLimit, context.fetchSize);
    }

    public static PlannerContext forSubPlan(PlannerContext context, int softLimit, int fetchSize) {
        return new PlannerContext(
            context.clusterState,
            context.routingProvider,
            UUID.randomUUID(),
            context.normalizer,
            context.transactionContext,
            softLimit,
            fetchSize
        );
    }

    private final UUID jobId;
    private final EvaluatingNormalizer normalizer;
    private final TransactionContext transactionContext;
    private final int softLimit;
    private final int fetchSize;
    private final RoutingBuilder routingBuilder;
    private final RoutingProvider routingProvider;
    private final ClusterState clusterState;
    private int executionPhaseId = 0;
    private final String handlerNode;

    public PlannerContext(ClusterState clusterState,
                          RoutingProvider routingProvider,
                          UUID jobId,
                          EvaluatingNormalizer normalizer,
                          TransactionContext transactionContext,
                          int softLimit,
                          int fetchSize) {
        this.routingProvider = routingProvider;
        this.routingBuilder = new RoutingBuilder(clusterState, routingProvider);
        this.clusterState = clusterState;
        this.jobId = jobId;
        this.normalizer = normalizer;
        this.transactionContext = transactionContext;
        this.softLimit = softLimit;
        this.fetchSize = fetchSize;
        this.handlerNode = clusterState.getNodes().getLocalNodeId();
    }

    public EvaluatingNormalizer normalizer() {
        return normalizer;
    }

    @Nullable
    public Integer toInteger(@Nullable Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        Input input = (Input) (normalizer.normalize(symbol, transactionContext));
        return DataTypes.INTEGER.value(input.value());
    }

    public int softLimit() {
        return softLimit;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public TransactionContext transactionContext() {
        return transactionContext;
    }

    void applySoftLimit(QuerySpec querySpec) {
        if (softLimit != 0 && querySpec.limit() == null) {
            querySpec.limit(Literal.of((long) softLimit));
        }
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

    public ReaderAllocations buildReaderAllocations() {
        return routingBuilder.buildReaderAllocations();
    }
}
