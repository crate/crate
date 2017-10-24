/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.action.FutureActionListener;
import io.crate.analyze.AlterTableAnalyzer;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.RerouteAllocateReplicaShardAnalyzedStatement;
import io.crate.analyze.RerouteAnalyzedStatement;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ShardedTable;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static io.crate.concurrent.CompletableFutures.failedFuture;

public final class RerouteActions {

    private RerouteActions() {

    }

    public static CompletableFuture<Long> execute(
        BiConsumer<ClusterRerouteRequest, ActionListener<ClusterRerouteResponse>> rerouteAction,
        RerouteAnalyzedStatement stmt,
        Row parameters) {

        ClusterRerouteRequest request;
        try {
            request = prepareRequest(stmt, parameters);
        } catch (Throwable t) {
            return failedFuture(t);
        }
        FutureActionListener<ClusterRerouteResponse, Long> listener =
            new FutureActionListener<>(r -> r.isAcknowledged() ? 1L : -1L);
        rerouteAction.accept(request, listener);
        return listener;
    }

    static ClusterRerouteRequest prepareRequest(RerouteAnalyzedStatement stmt, Row parameters) {
        return RequestBuilder.INSTANCE.process(stmt, parameters);
    }

    static String getRerouteIndex(RerouteAnalyzedStatement statement, Row parameters) throws IllegalArgumentException {
        ShardedTable shardedTable = statement.tableInfo();
        if (shardedTable instanceof DocTableInfo) {
            DocTableInfo docTableInfo = (DocTableInfo) shardedTable;
            String indexName = docTableInfo.ident().indexName();
            PartitionName partitionName = AlterTableAnalyzer.createPartitionName(statement.partitionProperties(),
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

    private static class RequestBuilder extends AnalyzedStatementVisitor<Row, ClusterRerouteRequest> {

        private static final RequestBuilder INSTANCE = new RequestBuilder();

        @Override
        protected ClusterRerouteRequest visitRerouteMoveShard(RerouteMoveShardAnalyzedStatement statement,
                                                              Row parameters) {
            String indexName = getRerouteIndex(statement, parameters);
            int shardId = ExpressionToNumberVisitor.convert(statement.shardId(), parameters).intValue();
            String fromNodeId = ExpressionToStringVisitor.convert(statement.fromNodeId(), parameters);
            String toNodeId = ExpressionToStringVisitor.convert(statement.toNodeId(), parameters);

            MoveAllocationCommand command = new MoveAllocationCommand(indexName, shardId, fromNodeId, toNodeId);
            ClusterRerouteRequest request = new ClusterRerouteRequest();
            request.add(command);
            return request;
        }

        @Override
        protected ClusterRerouteRequest visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShardAnalyzedStatement statement,
                                                                         Row parameters) {
            String indexName = getRerouteIndex(statement, parameters);
            int shardId = ExpressionToNumberVisitor.convert(statement.shardId(), parameters).intValue();
            String nodeId = ExpressionToStringVisitor.convert(statement.nodeId(), parameters);

            AllocateReplicaAllocationCommand command = new AllocateReplicaAllocationCommand(indexName, shardId, nodeId);
            ClusterRerouteRequest request = new ClusterRerouteRequest();
            request.add(command);
            return request;
        }
    }
}
