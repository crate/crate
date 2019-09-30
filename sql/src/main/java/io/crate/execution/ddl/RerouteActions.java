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

import io.crate.analyze.PartitionPropertiesAnalyzer;
import io.crate.analyze.PromoteReplicaStatement;
import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteAnalyzedStatement;
import io.crate.analyze.RerouteCancelShardAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ShardedTable;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.DataTypes;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;

import java.util.Locale;

import static io.crate.planner.NodeSelection.resolveNodeId;

public final class RerouteActions {

    private RerouteActions() {
    }

    static ClusterRerouteRequest prepareMoveShardReq(RerouteMoveShardAnalyzedStatement statement,
                                                     Row parameters,
                                                     DiscoveryNodes nodes) {
        String indexName = getRerouteIndex(statement, parameters);
        int shardId = ExpressionToNumberVisitor.convert(statement.shardId(), parameters).intValue();
        String fromNodeId = resolveNodeId(nodes, ExpressionToStringVisitor.convert(statement.fromNodeIdOrName(), parameters));
        String toNodeId = resolveNodeId(nodes, ExpressionToStringVisitor.convert(statement.toNodeIdOrName(), parameters));

        MoveAllocationCommand command = new MoveAllocationCommand(indexName, shardId, fromNodeId, toNodeId);
        ClusterRerouteRequest request = new ClusterRerouteRequest();
        request.add(command);
        return request;
    }

    static ClusterRerouteRequest prepareAllocateReplicaReq(RerouteAllocateReplicaShardAnalyzedStatement statement,
                                                           Row parameters,
                                                           DiscoveryNodes nodes) {
        String indexName = getRerouteIndex(statement, parameters);
        int shardId = ExpressionToNumberVisitor.convert(statement.shardId(), parameters).intValue();
        String nodeId = resolveNodeId(nodes, ExpressionToStringVisitor.convert(statement.nodeId(), parameters));

        AllocateReplicaAllocationCommand command = new AllocateReplicaAllocationCommand(indexName, shardId, nodeId);
        ClusterRerouteRequest request = new ClusterRerouteRequest();
        request.add(command);
        return request;
    }

    static ClusterRerouteRequest prepareCancelShardReq(RerouteCancelShardAnalyzedStatement statement,
                                                       Row parameters,
                                                       DiscoveryNodes nodes) {
        boolean allowPrimary = validateCancelRerouteProperty("allow_primary", statement.properties(), parameters);

        String indexName = getRerouteIndex(statement, parameters);
        int shardId = ExpressionToNumberVisitor.convert(statement.shardId(), parameters).intValue();
        String nodeId = resolveNodeId(nodes, ExpressionToStringVisitor.convert(statement.nodeId(), parameters));

        CancelAllocationCommand command = new CancelAllocationCommand(indexName, shardId, nodeId, allowPrimary);
        ClusterRerouteRequest request = new ClusterRerouteRequest();
        request.add(command);
        return request;
    }

    static ClusterRerouteRequest preparePromoteReplicaReq(PromoteReplicaStatement promoteReplica,
                                                          Functions functions,
                                                          Row parameters,
                                                          TransactionContext txnCtx,
                                                          DiscoveryNodes nodes) {
        String index = RerouteActions.getRerouteIndex(promoteReplica, parameters);
        Integer shardId = DataTypes.INTEGER.value(SymbolEvaluator.evaluate(
            txnCtx, functions, promoteReplica.shardId(), parameters, SubQueryResults.EMPTY));
        if (shardId == null) {
            throw new NullPointerException("Shard in REROUTE PROMOTE REPLICA must not be null");
        }
        Boolean acceptDataLoss = DataTypes.BOOLEAN.value(SymbolEvaluator.evaluate(
            txnCtx, functions, promoteReplica.acceptDataLoss(), parameters, SubQueryResults.EMPTY));
        if (acceptDataLoss == null) {
            throw new NullPointerException("`accept_data_loss` in REROUTE PROMOTE REPLICA must not be null");
        }
        String node = DataTypes.STRING.value(SymbolEvaluator.evaluate(
            txnCtx, functions, promoteReplica.node(), parameters, SubQueryResults.EMPTY));
        String nodeId = resolveNodeId(nodes, node);
        return new ClusterRerouteRequest()
            .add(new AllocateStalePrimaryAllocationCommand(index, shardId, nodeId, acceptDataLoss));
    }


    static long retryFailedAffectedShardsRowCount(ClusterRerouteResponse response) {
        if (response.isAcknowledged()) {
            long rowCount = 0L;
            for (RoutingNode routingNode : response.getState().getRoutingNodes()) {
                // filter shards with failed allocation attempts
                // failed allocation attempts can appear for shards with state UNASSIGNED and INITIALIZING
                rowCount += routingNode.shardsWithState(ShardRoutingState.UNASSIGNED, ShardRoutingState.INITIALIZING)
                    .stream()
                    .filter(s -> {
                        if (s.unassignedInfo() != null) {
                            return s.unassignedInfo().getReason().equals(UnassignedInfo.Reason.ALLOCATION_FAILED);
                        }
                        return false;
                    })
                    .count();
            }
            return rowCount;
        } else {
            return -1L;
        }
    }

    static String getRerouteIndex(RerouteAnalyzedStatement statement, Row parameters) throws IllegalArgumentException {
        ShardedTable shardedTable = statement.tableInfo();
        if (shardedTable instanceof DocTableInfo) {
            DocTableInfo docTableInfo = (DocTableInfo) shardedTable;
            String indexName = docTableInfo.ident().indexNameOrAlias();
            PartitionName partitionName = PartitionPropertiesAnalyzer.createPartitionName(statement.partitionProperties(),
                                                                                          docTableInfo, parameters);
            if (partitionName != null) {
                indexName = partitionName.asIndexName();
            } else if (docTableInfo.isPartitioned()) {
                throw new IllegalArgumentException("table is partitioned however no partition clause has been specified");
            }

            return indexName;
        }

        // Table is a blob table
        assert shardedTable.concreteIndices().length == 1 : "table has to contain only 1 index name";
        return shardedTable.concreteIndices()[0];
    }

    static boolean validateCancelRerouteProperty(String propertyKey, GenericProperties<Expression> properties, Row parameters) throws IllegalArgumentException {
        if (properties != null) {
            for (String key : properties.keys()) {
                if (propertyKey.equals(key)) {
                    return (boolean) ExpressionToObjectVisitor.convert(
                        properties.get(propertyKey),
                        parameters);
                } else {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "\"%s\" is not a valid setting for CANCEL SHARD", key));
                }
            }
        }
        return false;
    }
}
