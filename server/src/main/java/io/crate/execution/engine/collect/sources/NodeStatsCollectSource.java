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

package io.crate.execution.engine.collect.sources;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.node.Node;

import io.crate.analyze.WhereClause;
import io.crate.common.collections.Lists;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.collectors.NodeStats;
import io.crate.execution.engine.collect.stats.NodeStatsAction;
import io.crate.execution.engine.collect.stats.NodeStatsRequest;
import io.crate.execution.engine.collect.stats.NodeStatsResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.MapBackedRefResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.sys.SysNodesTableInfo;

@Singleton
public class NodeStatsCollectSource implements CollectSource {

    private final ActionExecutor<NodeStatsRequest, NodeStatsResponse> nodeStatsAction;
    private final ClusterService clusterService;
    private final NodeContext nodeCtx;
    private final InputFactory inputFactory;

    @Inject
    public NodeStatsCollectSource(Node node,
                                  ClusterService clusterService,
                                  NodeContext nodeCtx) {
        this.nodeStatsAction = req -> node.client().execute(NodeStatsAction.INSTANCE, req);
        this.clusterService = clusterService;
        this.inputFactory = new InputFactory(nodeCtx);
        this.nodeCtx = nodeCtx;
    }

    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                             CollectPhase phase,
                                                             CollectTask collectTask,
                                                             boolean supportMoveToStart) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        if (!WhereClause.canMatch(collectPhase.where())) {
            return completedFuture(InMemoryBatchIterator.empty(SentinelRow.SENTINEL));
        }
        Collection<DiscoveryNode> nodes = filterNodes(
            Lists.of(clusterService.state().nodes()),
            collectPhase.where(),
            nodeCtx);
        if (nodes.isEmpty()) {
            return completedFuture(InMemoryBatchIterator.empty(SentinelRow.SENTINEL));
        }
        return completedFuture(NodeStats.newInstance(nodeStatsAction, collectPhase, nodes, txnCtx, inputFactory));
    }

    static Collection<DiscoveryNode> filterNodes(Collection<DiscoveryNode> nodes, Symbol predicate, NodeContext nodeCtx) {
        var expressions = SysNodesTableInfo.INSTANCE.expressions();
        var nameExpr = expressions.get(SysNodesTableInfo.Columns.NAME).create();
        var idExpr = expressions.get(SysNodesTableInfo.Columns.ID).create();
        MapBackedRefResolver referenceResolver = new MapBackedRefResolver(Map.of(
            SysNodesTableInfo.Columns.NAME, nameExpr,
            SysNodesTableInfo.Columns.ID, idExpr)
        );
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.DOC,
            referenceResolver,
            null
        );
        List<DiscoveryNode> newNodes = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            String nodeId = node.getId();
            NodeStatsContext statsContext = new NodeStatsContext(nodeId, node.getName());
            nameExpr.setNextRow(statsContext);
            idExpr.setNextRow(statsContext);
            Symbol normalized = normalizer.normalize(predicate, CoordinatorTxnCtx.systemTransactionContext());
            if (normalized.equals(predicate)) {
                return nodes; // No local available sys nodes columns in where clause
            }
            if (WhereClause.canMatch(normalized)) {
                newNodes.add(node);
            }
        }
        return newNodes;
    }
}
