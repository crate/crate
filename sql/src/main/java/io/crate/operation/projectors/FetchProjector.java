/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongArrayList;
import io.crate.core.collections.Row;
import io.crate.executor.transport.*;
import io.crate.metadata.Functions;
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.fetch.PositionalBucketMerger;
import io.crate.operation.fetch.PositionalRowDelegate;
import io.crate.operation.fetch.RowInputSymbolVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchProjector implements Projector, RowDownstreamHandle {

    public static final int NO_BULK_REQUESTS = -1;

    private PositionalBucketMerger downstream;
    private final TransportFetchNodeAction transportFetchNodeAction;
    private final TransportCloseContextNodeAction transportCloseContextNodeAction;

    private final UUID jobId;
    private final CollectExpression<?> collectDocIdExpression;
    private final List<Reference> toFetchReferences;
    private final IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private final int bulkSize;
    private final Row outputRow;
    private final Map<String, NodeBucket> nodeBuckets = new HashMap<>();
    private final RowDelegate collectRow = new RowDelegate();
    private final RowDelegate fetchRow = new RowDelegate();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean consumingRows = new AtomicBoolean(true);
    private final Map<String, Integer> nodeIds;
    private final int numNodes;
    private final AtomicInteger remainingRequests = new AtomicInteger(0);

    private long inputCursor = 0;
    private int nodeIdIdx = 0;

    private static final ESLogger LOGGER = Loggers.getLogger(FetchProjector.class);

    public FetchProjector(TransportFetchNodeAction transportFetchNodeAction,
                          TransportCloseContextNodeAction transportCloseContextNodeAction,
                          Functions functions,
                          UUID jobId,
                          CollectExpression<?> collectDocIdExpression,
                          List<Symbol> inputSymbols,
                          List<Symbol> outputSymbols,
                          IntObjectOpenHashMap<String> jobSearchContextIdToNode,
                          int executionNodes,
                          int bulkSize) {
        this.transportFetchNodeAction = transportFetchNodeAction;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.jobId = jobId;
        this.collectDocIdExpression = collectDocIdExpression;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.bulkSize = bulkSize;
        numNodes = executionNodes;
        nodeIds = new HashMap<>(executionNodes);

        RowInputSymbolVisitor rowInputSymbolVisitor = new RowInputSymbolVisitor(functions);

        RowInputSymbolVisitor.Context collectRowContext = new RowInputSymbolVisitor.Context();
        collectRowContext.row(collectRow);

        RowInputSymbolVisitor.Context fetchRowContext = new RowInputSymbolVisitor.Context();
        fetchRowContext.row(fetchRow);

        // process input symbols (increase input index for every reference)
        for (Symbol symbol : inputSymbols) {
            rowInputSymbolVisitor.process(symbol, collectRowContext);
        }

        // process output symbols, use different contexts (and so different row delegates)
        // for collect(inputSymbols) & fetch
        List<Input<?>> inputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            if (inputSymbols.contains(symbol)) {
                inputs.add(rowInputSymbolVisitor.process(symbol, collectRowContext));
            } else {
                inputs.add(rowInputSymbolVisitor.process(symbol, fetchRowContext));
            }
        }
        toFetchReferences = fetchRowContext.references();

        outputRow = new InputRow(inputs);
    }

    @Override
    public void startProjection() {
        collectDocIdExpression.startCollect();

        if (remainingUpstreams.get() <= 0) {
            finish();
        } else {
            // register once to increment downstream upstreams counter
            downstream.registerUpstream(this);
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (!consumingRows.get()) {
            return false;
        }
        collectDocIdExpression.setNextRow(row);

        long docId = (Long)collectDocIdExpression.value();
        int jobSearchContextId = (int)(docId >> 32);

        String nodeId = jobSearchContextIdToNode.get(jobSearchContextId);
        Integer nodeIdx = nodeIds.get(nodeId);
        if (nodeIdx == null) {
            nodeIdx = nodeIdIdx;
            nodeIds.put(nodeId, nodeIdIdx++);
        }

        NodeBucket nodeBucket = nodeBuckets.get(nodeId);
        if (nodeBucket == null) {
            nodeBucket = new NodeBucket(nodeId, nodeIdx);
            nodeBuckets.put(nodeId, nodeBucket);

        }
        nodeBucket.add(inputCursor++, docId, row);
        if (bulkSize != NO_BULK_REQUESTS && nodeBucket.size() >= bulkSize) {
            flushNodeBucket(nodeBucket);
            nodeBuckets.remove(nodeBucket.nodeId);
        }

        return true;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = new PositionalBucketMerger(downstream, numNodes, outputRow.size());
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            // flush all remaining buckets
            Iterator<Map.Entry<String, NodeBucket>> it = nodeBuckets.entrySet().iterator();
            while (it.hasNext()) {
                flushNodeBucket(it.next().getValue());
                it.remove();
            }

            // projector registered itself as an upstream to prevent downstream of
            // flushing rows before all requests finished.
            // release it now as no new rows are consumed anymore (downstream will flush all remaining rows)
            downstream.finish();
        }
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }

    private void flushNodeBucket(final NodeBucket nodeBucket) {
        // every request must increase downstream upstream counter
        downstream.registerUpstream(this);
        remainingRequests.incrementAndGet();

        NodeFetchRequest request = new NodeFetchRequest();
        request.jobId(jobId);
        request.toFetchReferences(toFetchReferences);
        request.jobSearchContextDocIds(nodeBucket.docIds());
        if (bulkSize > NO_BULK_REQUESTS) {
            request.closeContext(false);
        }
        transportFetchNodeAction.execute(nodeBucket.nodeId, request, new ActionListener<NodeFetchResponse>() {
            @Override
            public void onResponse(NodeFetchResponse response) {
                List<Row> rows = new ArrayList<>(response.rows().size());
                int idx = 0;
                for (Row row : response.rows()) {
                    collectRow.delegate(nodeBucket.inputRow(idx));
                    fetchRow.delegate(row);
                    rows.add(new PositionalRowDelegate(outputRow, nodeBucket.cursor(idx)));
                    idx++;
                }
                if (!downstream.setNextBucket(rows, nodeBucket.nodeIdx)) {
                    consumingRows.set(false);
                }
                if (remainingRequests.decrementAndGet() <= 0 && remainingUpstreams.get() <= 0) {
                    closeContexts();
                }
                downstream.finish();
            }

            @Override
            public void onFailure(Throwable e) {
                downstream.fail(e);
            }
        });

    }

    /**
     * close job contexts on all affected nodes, just fire & forget, they will timeout anyway
     */
    private void closeContexts() {
        if (bulkSize > NO_BULK_REQUESTS) {
            LOGGER.trace("closing job context {} on {} nodes", jobId, numNodes);
            for (final String nodeId : nodeIds.keySet()) {
                transportCloseContextNodeAction.execute(nodeId, new NodeCloseContextRequest(jobId), new ActionListener<NodeCloseContextResponse>() {
                    @Override
                    public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.warn("Closing job context {} failed on node {} with: {}", jobId, nodeId, e.getMessage());
                    }
                });
            }
        }
    }

    private static class NodeBucket {

        private final int nodeIdx;
        private final String nodeId;
        private final List<Row> inputRows = new ArrayList<>();
        private final LongArrayList cursors = new LongArrayList();
        private final LongArrayList docIds = new LongArrayList();

        public NodeBucket(String nodeId, int nodeIdx) {
            this.nodeId = nodeId;
            this.nodeIdx = nodeIdx;
        }

        public void add(long cursor, Long docId, Row row) {
            cursors.add(cursor);
            docIds.add(docId);
            inputRows.add(row);
        }

        public int size() {
            return cursors.size();
        }

        public LongArrayList docIds() {
            return docIds;
        }

        public long cursor(int index) {
            return cursors.get(index);
        }

        public Row inputRow(int index) {
            return inputRows.get(index);
        }
    }

    private static class RowDelegate implements Row {
        private Row delegate;

        public void delegate(Row row) {
            delegate = row;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Object get(int index) {
            return delegate.get(index);
        }
    }

}
