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
import io.crate.concurrent.CompletableFutures;
import io.crate.data.*;
import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.NodeStatsResponse;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static io.crate.data.RowBridging.OFF_ROW;

/**
 * BatchIterator implementation that exposes the provided {@link RoutedCollectPhase} stats
 * of the given collection of nodes.
 */
public class NodeStatsIterator implements BatchIterator {

    private final TransportNodeStatsAction transportStatTablesAction;
    private final RoutedCollectPhase collectPhase;
    private final Collection<DiscoveryNode> nodes;
    private final InputFactory inputFactory;
    private CompletableFuture<Iterable<Row>> loading;
    private Iterable<Row> rows = Collections.emptyList();
    private Iterator<Row> it = rows.iterator();
    private final RowColumns rowData;

    private NodeStatsIterator(TransportNodeStatsAction transportStatTablesAction,
                              RoutedCollectPhase collectPhase,
                              Collection<DiscoveryNode> nodes,
                              InputFactory inputFactory) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
        this.inputFactory = inputFactory;
        rowData = new RowColumns(collectPhase.toCollect().size());
    }

    public static BatchIterator newInstance(TransportNodeStatsAction transportStatTablesAction,
                                            RoutedCollectPhase collectPhase,
                                            Collection<DiscoveryNode> nodes,
                                            InputFactory inputFactory) {
        NodeStatsIterator delegate = new NodeStatsIterator(transportStatTablesAction, collectPhase, nodes, inputFactory);
        return new CloseAssertingBatchIterator(delegate);
    }

    private CompletableFuture<List<NodeStatsContext>> getNodeStatsContexts() {
        Set<ColumnIdent> toCollect = TopLevelColumnIdentExtractor.extractColumns(collectPhase.toCollect());
        // If only ID or NAME are required it's possible to avoid collecting data from other nodes as everything
        // is available locally
        boolean collectDirectly = isCollectDirectlyPossible(toCollect);
        CompletableFuture<List<NodeStatsContext>> nodeStatsContextsFuture;
        if (collectDirectly) {
            nodeStatsContextsFuture = getNodeStatsContextFromLocalState();
        } else {
            nodeStatsContextsFuture = getNodeStatsContextFromRemoteState(toCollect);
        }
        return nodeStatsContextsFuture;
    }

    /**
     * @return true if all required column can be provided from the local state.
     */
    private boolean isCollectDirectlyPossible(Set<ColumnIdent> toCollect) {
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

    private CompletableFuture<List<NodeStatsContext>> getNodeStatsContextFromLocalState() {
        List<NodeStatsContext> rows = new ArrayList<>(nodes.size());
        for (DiscoveryNode node : nodes) {
            rows.add(new NodeStatsContext(node.getId(), node.name()));
        }
        return CompletableFuture.completedFuture(rows);
    }

    private CompletableFuture<List<NodeStatsContext>> getNodeStatsContextFromRemoteState(Set<ColumnIdent> toCollect) {
        final CompletableFuture<List<NodeStatsContext>> nodeStatsContextsFuture = new CompletableFuture<>();
        final List<NodeStatsContext> rows = Collections.synchronizedList(new ArrayList<NodeStatsContext>(nodes.size()));
        final AtomicInteger remainingNodesToCollect = new AtomicInteger(nodes.size());
        for (final DiscoveryNode node : nodes) {
            final String nodeId = node.getId();
            final NodeStatsRequest request = new NodeStatsRequest(toCollect);
            transportStatTablesAction.execute(nodeId, request, new ActionListener<NodeStatsResponse>() {
                @Override
                public void onResponse(NodeStatsResponse response) {
                    rows.add(response.nodeStatsContext());
                    if (remainingNodesToCollect.decrementAndGet() == 0) {
                        nodeStatsContextsFuture.complete(rows);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof ReceiveTimeoutTransportException) {
                        rows.add(new NodeStatsContext(nodeId, node.name()));
                        if (remainingNodesToCollect.decrementAndGet() == 0) {
                            nodeStatsContextsFuture.complete(rows);
                        }
                    } else {
                        nodeStatsContextsFuture.completeExceptionally(t);
                    }
                }
            }, TimeValue.timeValueMillis(3000L));
        }
        return nodeStatsContextsFuture;
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        it = rows.iterator();
        rowData.updateRef(OFF_ROW);
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        if (it.hasNext()) {
            rowData.updateRef(it.next());
            return true;
        }
        rowData.updateRef(OFF_ROW);
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (isLoading()) {
            return CompletableFutures.failedFuture(new IllegalStateException("Iterator is already loading"));
        }

        if (allLoaded()) {
            return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
        }

        loading = new CompletableFuture<>();

        CompletableFuture<List<NodeStatsContext>> nodeStatsContexts = getNodeStatsContexts();
        nodeStatsContexts.whenComplete((List<NodeStatsContext> result, Throwable t) -> {
            if (t == null) {
                rows = RowsTransformer.toRowsIterable(inputFactory, RowContextReferenceResolver.INSTANCE, collectPhase,
                    result);
                it = rows.iterator();
                loading.complete(rows);
            } else {
                loading.completeExceptionally(t);
            }
        });

        return loading;
    }

    @Override
    public boolean allLoaded() {
        return loading != null;
    }

    private boolean isLoading() {
        return loading != null && loading.isDone() == false;
    }

    private void raiseIfLoading() {
        if (isLoading()) {
            throw new IllegalStateException("Iterator is loading");
        }
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        // cannot cancel remote request and if the data is already here this is fast enough to ignore the kill.
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
