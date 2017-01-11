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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.collect.collectors.NodeStatsCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.operation.reference.sys.node.local.NodeSysReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Singleton
public class NodeStatsCollectSource implements CollectSource {

    private final TransportActionProvider transportActionProvider;
    private final NodeSysExpression nodeSysExpression;
    private final ClusterService clusterService;
    private final Functions functions;
    private final InputFactory inputFactory;

    @Inject
    public NodeStatsCollectSource(TransportActionProvider transportActionProvider,
                                  NodeSysExpression nodeSysExpression,
                                  ClusterService clusterService,
                                  Functions functions) {
        this.transportActionProvider = transportActionProvider;
        this.nodeSysExpression = nodeSysExpression;
        this.clusterService = clusterService;
        this.inputFactory = new InputFactory(functions);
        this.functions = functions;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions, RowGranularity.DOC, ReplaceMode.COPY, new NodeSysReferenceResolver(nodeSysExpression), null);
        collectPhase = collectPhase.normalize(normalizer, null);
        if (collectPhase.whereClause().noMatch()) {
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }
        Collection<DiscoveryNode> nodes = nodeIds(collectPhase.whereClause(),
            Lists.newArrayList(clusterService.state().getNodes().iterator()),
            functions);
        if (nodes.isEmpty()) {
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }
        return ImmutableList.<CrateCollector>of(new NodeStatsCollector(
                transportActionProvider.transportStatTablesActionProvider(),
                downstream,
                collectPhase,
                nodes,
                inputFactory
            )
        );
    }

    @Nullable
    static Collection<DiscoveryNode> nodeIds(WhereClause whereClause,
                                             Collection<DiscoveryNode> nodes,
                                             Functions functions) {
        if (!whereClause.hasQuery()) {
            return nodes;
        }
        LocalSysColReferenceResolver localSysColReferenceResolver = new LocalSysColReferenceResolver(
            ImmutableList.of(SysNodesTableInfo.Columns.NAME, SysNodesTableInfo.Columns.ID)
        );
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.DOC,
            ReplaceMode.COPY,
            localSysColReferenceResolver,
            null
        );
        List<DiscoveryNode> newNodes = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            String nodeId = node.getId();
            for (RowCollectExpression expression : localSysColReferenceResolver.expressions()) {
                expression.setNextRow(new NodeStatsContext(nodeId, node.name()));
            }
            Symbol normalized = normalizer.normalize(whereClause.query(), null);
            if (normalized.equals(whereClause.query())) {
                return nodes; // No local available sys nodes columns in where clause
            }
            if (WhereClause.canMatch(normalized)) {
                newNodes.add(node);
            }
        }
        return newNodes;
    }
}
