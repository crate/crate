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
import io.crate.metadata.RowCollectExpression;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;
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
    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor;
    private final TopLevelColumnIdentVisitor topLevelColumnIdentVisitor = TopLevelColumnIdentVisitor.INSTANCE;
    private final AtomicInteger remainingRequests = new AtomicInteger();

    public NodeStatsCollector(TransportNodeStatsAction transportStatTablesAction,
                              RowReceiver rowReceiver,
                              RoutedCollectPhase collectPhase,
                              Collection<DiscoveryNode> nodes,
                              CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.rowReceiver = rowReceiver;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
        this.inputSymbolVisitor = inputSymbolVisitor;
    }

    @Override
    public void doCollect() {
        remainingRequests.set(nodes.size());
        final List<NodeStatsContext> rows = Collections.synchronizedList(new ArrayList<NodeStatsContext>());
        for (final DiscoveryNode node : nodes) {
            Set<ColumnIdent> toCollect = topLevelColumnIdentVisitor.process(collectPhase.toCollect());
            final NodeStatsRequest request = new NodeStatsRequest(toCollect);

            transportStatTablesAction.execute(node.id(), request, new ActionListener<NodeStatsResponse>() {
                @Override
                public void onResponse(NodeStatsResponse response) {
                    rows.add(response.nodeStatsContext());
                    if (remainingRequests.decrementAndGet() == 0) {
                        emmitRows(rows);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof ReceiveTimeoutTransportException) {
                        rows.add(new NodeStatsContext(node.id(), node.name()));
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
            RowsTransformer.toRowsIterable(inputSymbolVisitor, collectPhase, rows)
        ).run();
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
    }

    private static class TopLevelColumnIdentVisitor extends DefaultTraversalSymbolVisitor<TopLevelColumnIdentVisitor.Context, Void> {

        static final TopLevelColumnIdentVisitor INSTANCE = new TopLevelColumnIdentVisitor();

        static class Context {

            private final Set<ColumnIdent> columnIdents = new HashSet<>();

            void add(ColumnIdent columnIdent) {
                columnIdents.add(columnIdent);
            }

            Set<ColumnIdent> columnIdents() {
                return columnIdents;
            }
        }

        Set<ColumnIdent> process(Collection<? extends Symbol> symbols) {
            Context context = new Context();
            for (Symbol symbol : symbols) {
                process(symbol, context);
            }
            return context.columnIdents();
        }

        @Override
        public Void visitReference(Reference symbol, Context context) {
            context.add(symbol.ident().columnReferenceIdent().columnIdent());
            return null;
        }
    }
}
