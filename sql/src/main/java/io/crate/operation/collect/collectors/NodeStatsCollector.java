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

package io.crate.operation.collect.collectors;

import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.NodeStatsResponse;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeStatsCollector implements CrateCollector {

    private final TransportNodeStatsAction transportStatTablesAction;
    private final RowReceiver rowReceiver;
    private final RoutedCollectPhase collectPhase;
    private final Collection<DiscoveryNode> nodes;
    private final InputFactory inputFactory;

    public NodeStatsCollector(TransportNodeStatsAction transportStatTablesAction,
                              RowReceiver rowReceiver,
                              RoutedCollectPhase collectPhase,
                              Collection<DiscoveryNode> nodes,
                              InputFactory inputFactory) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.rowReceiver = rowReceiver;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
        this.inputFactory = inputFactory;
    }

    @Override
    public void doCollect() {
        Set<ColumnIdent> toCollect = TopLevelColumnIdentExtractor.extractColumns(collectPhase.toCollect());
        // If only ID or NAME are required it's possible to avoid collecting data from other nodes as everything
        // is available locally
        boolean emitDirectly = isEmitDirectlyPossible(toCollect);
        if (emitDirectly) {
            emitAllFromLocalState();
        } else {
            retrieveRemoteStateAndEmit(toCollect);
        }
    }

    /**
     * @return true if all required column can be provided from the local state.
     */
    private boolean isEmitDirectlyPossible(Set<ColumnIdent> toCollect) {
        switch (toCollect.size()) {
            case 1:
                return toCollect.contains(SysNodesTableInfo.Columns.ID) ||
                       toCollect.contains(SysNodesTableInfo.Columns.NAME);
            case 2:
                return toCollect.contains(SysNodesTableInfo.Columns.ID) &&
                       toCollect.contains(SysNodesTableInfo.Columns.NAME);
        }
        return false;
    }

    private void emitAllFromLocalState() {
        List<NodeStatsContext> rows = new ArrayList<>(nodes.size());
        for (DiscoveryNode node : nodes) {
            rows.add(new NodeStatsContext(node.getId(), node.getName()));
        }
        emmitRows(rows);
    }

    private void retrieveRemoteStateAndEmit(Set<ColumnIdent> toCollect) {
        final AtomicInteger remainingRequests = new AtomicInteger(nodes.size());
        final List<NodeStatsContext> rows = Collections.synchronizedList(new ArrayList<NodeStatsContext>(nodes.size()));
        for (final DiscoveryNode node : nodes) {
            final String nodeId = node.getId();
            final NodeStatsRequest request = new NodeStatsRequest(toCollect);
            transportStatTablesAction.execute(nodeId, request, new ActionListener<NodeStatsResponse>() {
                @Override
                public void onResponse(NodeStatsResponse response) {
                    rows.add(response.nodeStatsContext());
                    if (remainingRequests.decrementAndGet() == 0) {
                        emmitRows(rows);
                    }
                }

                @Override
                public void onFailure(Exception t) {
                    if (t instanceof ReceiveTimeoutTransportException) {
                        rows.add(new NodeStatsContext(nodeId, node.getName()));
                        if (remainingRequests.decrementAndGet() == 0) {
                            emmitRows(rows);
                        }
                    } else {
                        rowReceiver.fail(t);
                    }
                }
            }, TimeValue.timeValueMillis(3000L));
        }
    }

    private void emmitRows(List<NodeStatsContext> rows) {
        new IterableRowEmitter(
            rowReceiver,
            RowsTransformer.toRowsIterable(inputFactory, RowContextReferenceResolver.INSTANCE, collectPhase, rows)
        ).run();
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
    }

    private static class TopLevelColumnIdentExtractor extends DefaultTraversalSymbolVisitor<Set<ColumnIdent>, Void> {

        static final TopLevelColumnIdentExtractor INSTANCE = new TopLevelColumnIdentExtractor();

        static Set<ColumnIdent> extractColumns(Iterable<? extends Symbol> symbols) {
            Set<ColumnIdent> columns = new HashSet<>();
            for (Symbol symbol : symbols) {
                INSTANCE.process(symbol, columns);
            }
            return columns;
        }

        @Override
        public Void visitReference(Reference symbol, Set<ColumnIdent> context) {
            context.add(symbol.ident().columnIdent().getRoot());
            return null;
        }
    }
}
