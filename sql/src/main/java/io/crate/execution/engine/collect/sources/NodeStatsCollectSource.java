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

package io.crate.execution.engine.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.QueryClause;
import io.crate.analyze.WhereClause;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.execution.engine.collect.collectors.NodeStats;
import io.crate.execution.engine.collect.stats.TransportNodeStatsAction;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.LocalSysColReferenceResolver;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.SysNodesTableInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Singleton
public class NodeStatsCollectSource implements CollectSource {

    private final TransportNodeStatsAction nodeStatsAction;
    private final ClusterService clusterService;
    private final Functions functions;
    private final InputFactory inputFactory;

    @Inject
    public NodeStatsCollectSource(TransportNodeStatsAction nodeStatsAction,
                                  ClusterService clusterService,
                                  Functions functions) {
        this.nodeStatsAction = nodeStatsAction;
        this.clusterService = clusterService;
        this.inputFactory = new InputFactory(functions);
        this.functions = functions;
    }

    @Override
    public BatchIterator<Row> getIterator(TransactionContext txnCtx,
                                          CollectPhase phase,
                                          CollectTask collectTask,
                                          boolean supportMoveToStart) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        if (!QueryClause.canMatch(collectPhase.where())) {
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        Collection<DiscoveryNode> nodes = nodeIds(
            collectPhase.where(),
            Lists.newArrayList(clusterService.state().getNodes().iterator()),
            functions);
        if (nodes.isEmpty()) {
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        return NodeStats.newInstance(nodeStatsAction, collectPhase, nodes, txnCtx, inputFactory);
    }

    @Nullable
    static Collection<DiscoveryNode> nodeIds(Symbol where,
                                             Collection<DiscoveryNode> nodes,
                                             Functions functions) {
        LocalSysColReferenceResolver localSysColReferenceResolver = new LocalSysColReferenceResolver(
            ImmutableList.of(SysNodesTableInfo.Columns.NAME, SysNodesTableInfo.Columns.ID)
        );
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.DOC,
            localSysColReferenceResolver,
            null
        );
        List<DiscoveryNode> newNodes = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            String nodeId = node.getId();
            for (NestableCollectExpression<NodeStatsContext, ?> expression : localSysColReferenceResolver.expressions()) {
                expression.setNextRow(new NodeStatsContext(nodeId, node.getName()));
            }
            Symbol normalized = normalizer.normalize(where, null);
            if (normalized.equals(where)) {
                return nodes; // No local available sys nodes columns in where clause
            }
            if (WhereClause.canMatch(normalized)) {
                newNodes.add(node);
            }
        }
        return newNodes;
    }
}
