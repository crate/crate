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

package io.crate.planner.node.management;

import static io.crate.planner.NodeSelection.resolveNodeId;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;

import io.crate.analyze.AnalyzedPromoteReplica;
import io.crate.analyze.AnalyzedRerouteAllocateReplicaShard;
import io.crate.analyze.AnalyzedRerouteCancelShard;
import io.crate.analyze.AnalyzedRerouteMoveShard;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ShardedTable;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.DataTypes;

public class AlterTableReroutePlan implements Plan {

    private static final InnerVisitor REROUTE_STATEMENTS_VISITOR = new InnerVisitor();
    private final AnalyzedStatement rerouteStatement;

    public AlterTableReroutePlan(AnalyzedStatement rerouteStatement) {
        this.rerouteStatement = rerouteStatement;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        var rerouteCommand = createRerouteCommand(
            rerouteStatement,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            params,
            subQueryResults,
            dependencies.clusterService().state().nodes());

        dependencies.client().execute(ClusterRerouteAction.INSTANCE, new ClusterRerouteRequest().add(rerouteCommand))
            .whenComplete(new OneRowActionListener<>(consumer, r -> new Row1(r == null ? -1L : 1L)));
    }

    @VisibleForTesting
    public static AllocationCommand createRerouteCommand(AnalyzedStatement reroute,
                                                         CoordinatorTxnCtx txnCtx,
                                                         NodeContext nodeCtx,
                                                         Row parameters,
                                                         SubQueryResults subQueryResults,
                                                         DiscoveryNodes nodes) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        return reroute.accept(
            REROUTE_STATEMENTS_VISITOR,
            new Context(nodes, eval));
    }

    private static class Context {

        private final DiscoveryNodes nodes;
        private final Function<? super Symbol, Object> eval;

        Context(DiscoveryNodes nodes,
                Function<? super Symbol, Object> eval) {
            this.nodes = nodes;
            this.eval = eval;
        }
    }

    private static class InnerVisitor extends AnalyzedStatementVisitor<Context, AllocationCommand> {

        @Override
        protected AllocationCommand visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Context context) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement));
        }

        @Override
        public AllocationCommand visitReroutePromoteReplica(AnalyzedPromoteReplica statement,
                                                            Context context) {
            var boundedPromoteReplica = statement.promoteReplica().map(context.eval);
            validateShardId(boundedPromoteReplica.shardId());

            String index = getRerouteIndex(
                statement.shardedTable(),
                Lists.map(statement.partitionProperties(), x -> x.map(context.eval)));
            String toNodeId = resolveNodeId(
                context.nodes,
                DataTypes.STRING.sanitizeValue(boundedPromoteReplica.node()));

            return new AllocateStalePrimaryAllocationCommand(
                index,
                DataTypes.INTEGER.sanitizeValue(boundedPromoteReplica.shardId()),
                toNodeId,
                DataTypes.BOOLEAN.sanitizeValue(context.eval.apply(statement.acceptDataLoss()))
            );

        }

        @Override
        protected AllocationCommand visitRerouteMoveShard(AnalyzedRerouteMoveShard statement,
                                                          Context context) {
            var boundedMoveShard = statement.rerouteMoveShard().map(context.eval);
            validateShardId(boundedMoveShard.shardId());

            String index = getRerouteIndex(
                statement.shardedTable(),
                Lists.map(statement.partitionProperties(), x -> x.map(context.eval)));
            String toNodeId = resolveNodeId(
                context.nodes,
                DataTypes.STRING.sanitizeValue(boundedMoveShard.toNodeIdOrName()));

            return new MoveAllocationCommand(
                index,
                DataTypes.INTEGER.sanitizeValue(boundedMoveShard.shardId()),
                DataTypes.STRING.sanitizeValue(boundedMoveShard.fromNodeIdOrName()),
                toNodeId
            );
        }

        @Override
        protected AllocationCommand visitRerouteAllocateReplicaShard(AnalyzedRerouteAllocateReplicaShard statement,
                                                                     Context context) {
            var boundedRerouteAllocateReplicaShard = statement
                .rerouteAllocateReplicaShard()
                .map(context.eval);
            validateShardId(boundedRerouteAllocateReplicaShard.shardId());

            String index = getRerouteIndex(
                statement.shardedTable(),
                Lists.map(statement.partitionProperties(), x -> x.map(context.eval)));
            String toNodeId = resolveNodeId(
                context.nodes,
                DataTypes.STRING.sanitizeValue(boundedRerouteAllocateReplicaShard.nodeIdOrName()));

            return new AllocateReplicaAllocationCommand(
                index,
                DataTypes.INTEGER.sanitizeValue(boundedRerouteAllocateReplicaShard.shardId()),
                toNodeId
            );
        }

        @Override
        protected AllocationCommand visitRerouteCancelShard(AnalyzedRerouteCancelShard statement,
                                                            Context context) {
            var boundedRerouteCancelShard = statement
                .rerouteCancelShard()
                .map(context.eval);
            validateShardId(boundedRerouteCancelShard.shardId());

            boolean allowPrimary = validateCancelRerouteProperty(
                "allow_primary", boundedRerouteCancelShard.properties());

            String index = getRerouteIndex(
                statement.shardedTable(),
                Lists.map(statement.partitionProperties(), x -> x.map(context.eval)));
            String nodeId = resolveNodeId(
                context.nodes,
                DataTypes.STRING.sanitizeValue(boundedRerouteCancelShard.nodeIdOrName()));

            return new CancelAllocationCommand(
                index,
                DataTypes.INTEGER.sanitizeValue(boundedRerouteCancelShard.shardId()),
                nodeId,
                allowPrimary
            );
        }

        private static String getRerouteIndex(ShardedTable shardedTable,
                                              List<Assignment<Object>> partitionsProperties) {
            if (shardedTable instanceof DocTableInfo) {
                DocTableInfo docTableInfo = (DocTableInfo) shardedTable;

                String indexName = docTableInfo.ident().indexNameOrAlias();
                PartitionName partitionName = PartitionPropertiesAnalyzer
                    .createPartitionName(partitionsProperties, docTableInfo);
                if (partitionName != null) {
                    indexName = partitionName.asIndexName();
                } else if (docTableInfo.isPartitioned()) {
                    throw new IllegalArgumentException(
                        "table is partitioned however no partition clause has been specified");
                }

                return indexName;
            }

            // Table is a blob table
            assert shardedTable.concreteIndices().length == 1 : "table has to contain only 1 index name";
            return shardedTable.concreteIndices()[0];
        }

        private static boolean validateCancelRerouteProperty(String propertyKey,
                                                             GenericProperties<Object> properties) {
            for (String key : properties.keys()) {
                if (propertyKey.equals(key)) {
                    return DataTypes.BOOLEAN.sanitizeValue(properties.get(propertyKey));
                } else {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "\"%s\" is not a valid setting for CANCEL SHARD", key));
                }
            }
            return false;
        }

        private void validateShardId(Object shardId) {
            if (shardId == null) {
                throw new IllegalArgumentException("Shard Id cannot be [null]");
            }
        }
    }
}
