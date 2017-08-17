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
import io.crate.data.BatchIterator;
import io.crate.data.CloseAssertingBatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.NodeStatsResponse;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.reference.StaticTableReferenceResolver;
import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * BatchIterator implementation that exposes the provided {@link RoutedCollectPhase} stats
 * of the given collection of nodes.
 */
public class NodeStatsIterator implements BatchIterator<Row> {

    private final TransportNodeStatsAction transportStatTablesAction;
    private final RoutedCollectPhase collectPhase;
    private final Collection<DiscoveryNode> nodes;
    private final InputFactory inputFactory;
    private CompletableFuture<Iterable<Row>> loading;
    private Iterable<Row> rows = Collections.emptyList();
    private Iterator<Row> it = rows.iterator();
    private Row current;

    private NodeStatsIterator(TransportNodeStatsAction transportStatTablesAction,
                              RoutedCollectPhase collectPhase,
                              Collection<DiscoveryNode> nodes,
                              InputFactory inputFactory) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
        this.inputFactory = inputFactory;
    }

    public static BatchIterator<Row> newInstance(TransportNodeStatsAction transportStatTablesAction,
                                                 RoutedCollectPhase collectPhase,
                                                 Collection<DiscoveryNode> nodes,
                                                 InputFactory inputFactory) {
        NodeStatsIterator delegate = new NodeStatsIterator(transportStatTablesAction, collectPhase, nodes, inputFactory);
        return new CloseAssertingBatchIterator<>(delegate);
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
            rows.add(new NodeStatsContext(node.getId(), node.getName()));
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
                public void onFailure(Exception e) {
                    Throwable t = SQLExceptions.unwrap(e);
                    if (isTimeoutOrNodeNotReachable(t)) {
                        rows.add(new NodeStatsContext(nodeId, node.getName()));
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

    private static boolean isTimeoutOrNodeNotReachable(Throwable t) {
        return t instanceof ReceiveTimeoutTransportException
            || t instanceof ConnectTransportException;
    }

    @Override
    public Row currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        it = rows.iterator();
        current = null;
    }

    @Override
    public boolean moveNext() {
        if (it.hasNext()) {
            current = it.next();
            return true;
        }
        current = null;
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (allLoaded()) {
            return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
        }

        loading = new CompletableFuture<>();

        StaticTableReferenceResolver<NodeStatsContext> referenceResolver = new StaticTableReferenceResolver<>(
            SysNodesTableInfo.expressions());
        CompletableFuture<List<NodeStatsContext>> nodeStatsContexts = getNodeStatsContexts();
        nodeStatsContexts.whenComplete((List<NodeStatsContext> result, Throwable t) -> {
            if (t == null) {
                rows = RowsTransformer.toRowsIterable(inputFactory,referenceResolver, collectPhase,
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
