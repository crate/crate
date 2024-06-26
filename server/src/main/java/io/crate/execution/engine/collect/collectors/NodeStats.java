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

package io.crate.execution.engine.collect.collectors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import io.crate.common.unit.TimeValue;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.RowsTransformer;
import io.crate.execution.engine.collect.stats.NodeStatsRequest;
import io.crate.execution.engine.collect.stats.NodeStatsResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;


/**
 * Collects information for sys.nodes locally or remote based on the provided {@link RoutedCollectPhase}
 */
public final class NodeStats {

    public static BatchIterator<Row> newInstance(ActionExecutor<NodeStatsRequest, NodeStatsResponse> nodeStatesAction,
                                                 RoutedCollectPhase collectPhase,
                                                 Collection<DiscoveryNode> nodes,
                                                 TransactionContext txnCtx,
                                                 InputFactory inputFactory) {

        return CollectingBatchIterator.newInstance(
            () -> {},
            t -> {},
            new LoadNodeStats(
                nodeStatesAction,
                collectPhase,
                nodes,
                txnCtx,
                inputFactory
            ),
            true
        );
    }

    private static final class LoadNodeStats implements Supplier<CompletableFuture<? extends Iterable<? extends Row>>> {

        private static final TimeValue REQUEST_TIMEOUT = TimeValue.timeValueMillis(3000L);
        private final ActionExecutor<NodeStatsRequest, NodeStatsResponse> nodeStatsAction;
        private final RoutedCollectPhase collectPhase;
        private final Collection<DiscoveryNode> nodes;
        private final TransactionContext txnCtx;
        private final InputFactory inputFactory;
        private final Map<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>> expressions;

        LoadNodeStats(ActionExecutor<NodeStatsRequest, NodeStatsResponse> nodeStatsAction,
                      RoutedCollectPhase collectPhase,
                      Collection<DiscoveryNode> nodes,
                      TransactionContext txnCtx,
                      InputFactory inputFactory) {
            this.nodeStatsAction = nodeStatsAction;
            this.collectPhase = collectPhase;
            this.nodes = nodes;
            this.txnCtx = txnCtx;
            this.inputFactory = inputFactory;
            this.expressions = SysNodesTableInfo.INSTANCE.expressions();
        }

        @Override
        public CompletableFuture<Iterable<Row>> get() {
            StaticTableReferenceResolver<NodeStatsContext> referenceResolver =
                new StaticTableReferenceResolver<>(expressions);
            return getNodeStatsContexts()
                .thenApply(result -> RowsTransformer.toRowsIterable(txnCtx, inputFactory, referenceResolver, collectPhase, result));
        }

        private CompletableFuture<List<NodeStatsContext>> getNodeStatsContexts() {
            Set<ColumnIdent> toCollect = getRootColumns(collectPhase.toCollect());
            toCollect.addAll(getRootColumns(List.of(collectPhase.where())));
            return dataAvailableInClusterState(toCollect)
                ? getStatsFromLocalState()
                : getStatsFromRemote(toCollect);
        }

        private CompletableFuture<List<NodeStatsContext>> getStatsFromLocalState() {
            List<NodeStatsContext> rows = new ArrayList<>(nodes.size());
            for (DiscoveryNode node : nodes) {
                rows.add(new NodeStatsContext(node.getId(), node.getName()));
            }
            return CompletableFuture.completedFuture(rows);
        }

        private CompletableFuture<List<NodeStatsContext>> getStatsFromRemote(Set<ColumnIdent> toCollect) {
            final CompletableFuture<List<NodeStatsContext>> nodeStatsContextsFuture = new CompletableFuture<>();
            final List<NodeStatsContext> rows = new ArrayList<>(nodes.size());
            final AtomicInteger remainingNodesToCollect = new AtomicInteger(nodes.size());
            for (final DiscoveryNode node : nodes) {
                final String nodeId = node.getId();
                NodeStatsRequest request = new NodeStatsRequest(nodeId, REQUEST_TIMEOUT, toCollect);
                nodeStatsAction
                    .execute(request)
                    .whenComplete(
                        (resp, t) -> {
                            if (t == null) {
                                synchronized (rows) {
                                    rows.add(resp.nodeStatsContext());
                                }
                                if (remainingNodesToCollect.decrementAndGet() == 0) {
                                    nodeStatsContextsFuture.complete(rows);
                                }
                            } else {
                                Throwable ut = SQLExceptions.unwrap(t);
                                if (isTimeoutOrNodeNotReachable(ut)) {
                                    NodeStatsContext statsContext = new NodeStatsContext(nodeId, node.getName());
                                    synchronized (rows) {
                                        rows.add(statsContext);
                                    }
                                    if (remainingNodesToCollect.decrementAndGet() == 0) {
                                        nodeStatsContextsFuture.complete(rows);
                                    }
                                } else {
                                    nodeStatsContextsFuture.completeExceptionally(ut);
                                }
                            }
                        }
                    );
            }
            return nodeStatsContextsFuture;
        }
    }

    private static boolean isTimeoutOrNodeNotReachable(Throwable t) {
        return t instanceof ReceiveTimeoutTransportException
            || t instanceof ConnectTransportException;
    }

    /**
     * @return true if all required column can be provided from the local state.
     */
    private static boolean dataAvailableInClusterState(Set<ColumnIdent> toCollect) {
        switch (toCollect.size()) {
            case 1:
                return toCollect.contains(SysNodesTableInfo.Columns.ID) ||
                       toCollect.contains(SysNodesTableInfo.Columns.NAME);
            case 2:
                return toCollect.contains(SysNodesTableInfo.Columns.ID) &&
                       toCollect.contains(SysNodesTableInfo.Columns.NAME);
            default:
                return false;
        }
    }

    private static Set<ColumnIdent> getRootColumns(Iterable<? extends Symbol> symbols) {
        HashSet<ColumnIdent> columns = new HashSet<>();
        Consumer<Reference> addRootColumn = ref -> columns.add(ref.column().getRoot());
        for (Symbol symbol: symbols) {
            symbol.visit(Reference.class, addRootColumn);
        }
        return columns;
    }
}
