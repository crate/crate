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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.*;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfo;
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.fetch.PositionalBucketMerger;
import io.crate.operation.fetch.PositionalRowDelegate;
import io.crate.operation.fetch.RowInputSymbolVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchProjector implements Projector, RowDownstreamHandle {

    public static final int NO_BULK_REQUESTS = -1;


    private PositionalBucketMerger downstream;
    private final TransportFetchNodeAction transportFetchNodeAction;
    private final TransportCloseContextNodeAction transportCloseContextNodeAction;

    private final UUID jobId;
    private final int executionNodeId;
    private final CollectExpression<?> collectDocIdExpression;
    private final List<ReferenceInfo> partitionedBy;
    private final List<Reference> toFetchReferences;
    private final IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private final IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard;
    private final int bulkSize;
    private final RowDelegate collectRowDelegate = new RowDelegate();
    private final RowDelegate fetchRowDelegate = new RowDelegate();
    private final RowDelegate partitionRowDelegate = new RowDelegate();
    private final Object rowDelegateLock = new Object();
    private final Row outputRow;
    private final Map<Integer, NodeBucket> nodeBuckets = new HashMap<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean consumingRows = new AtomicBoolean(true);
    private final List<String> executionNodes;
    private final int numNodes;
    private final AtomicInteger remainingRequests = new AtomicInteger(0);
    private final Map<String, Row> partitionRowsCache = new HashMap<>();
    private final Object partitionRowsCacheLock = new Object();
    private final Set<String> nodesWithOpenContext;

    private int inputCursor = 0;
    private boolean needInputRow = false;

    private static final ESLogger LOGGER = Loggers.getLogger(FetchProjector.class);

    public FetchProjector(TransportFetchNodeAction transportFetchNodeAction,
                          TransportCloseContextNodeAction transportCloseContextNodeAction,
                          Functions functions,
                          UUID jobId,
                          int executionNodeId,
                          CollectExpression<?> collectDocIdExpression,
                          List<Symbol> inputSymbols,
                          List<Symbol> outputSymbols,
                          List<ReferenceInfo> partitionedBy,
                          IntObjectOpenHashMap<String> jobSearchContextIdToNode,
                          IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard,
                          Set<String> executionNodes,
                          int bulkSize,
                          boolean closeContexts) {
        this.transportFetchNodeAction = transportFetchNodeAction;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.jobId = jobId;
        this.executionNodeId = executionNodeId;
        this.collectDocIdExpression = collectDocIdExpression;
        this.partitionedBy = partitionedBy;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.jobSearchContextIdToShard = jobSearchContextIdToShard;
        this.bulkSize = bulkSize;
        this.closeContexts = closeContexts;
        numNodes = executionNodes.size();
        this.executionNodes = new ArrayList<>(executionNodes);


        nodesWithOpenContext = new HashSet<>(jobSearchContextIdToNode.values().size());
        for (ObjectCursor<String> cursor : jobSearchContextIdToNode.values()) {
            nodesWithOpenContext.add(cursor.value);
        }

        RowInputSymbolVisitor rowInputSymbolVisitor = new RowInputSymbolVisitor(functions);

        RowInputSymbolVisitor.Context collectRowContext = new RowInputSymbolVisitor.Context();
        collectRowContext.row(collectRowDelegate);
        collectRowContext.partitionedBy(partitionedBy);
        collectRowContext.partitionByRow(partitionRowDelegate);

        RowInputSymbolVisitor.Context fetchRowContext = new RowInputSymbolVisitor.Context();
        fetchRowContext.row(fetchRowDelegate);
        fetchRowContext.partitionedBy(partitionedBy);
        fetchRowContext.partitionByRow(partitionRowDelegate);

        // process input symbols (increase input index for every reference)
        for (Symbol symbol : inputSymbols) {
            rowInputSymbolVisitor.process(symbol, collectRowContext);
        }

        // process output symbols, use different contexts (and so different row delegates)
        // for collect(inputSymbols) & fetch
        List<Input<?>> inputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            if (inputSymbols.contains(symbol)) {
                needInputRow = true;
                inputs.add(rowInputSymbolVisitor.process(symbol, collectRowContext));
            } else {
                inputs.add(rowInputSymbolVisitor.process(symbol, fetchRowContext));
            }
        }
        toFetchReferences = fetchRowContext.references();

        outputRow = new InputRow(inputs);
    }

    @Override
    public void startProjection(ExecutionState executionState) {
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
        String index = jobSearchContextIdToShard.get(jobSearchContextId).getIndex();
        int nodeIdIndex = Objects.hash(nodeId);

        NodeBucket nodeBucket = nodeBuckets.get(nodeIdIndex);
        if (nodeBucket == null) {
            nodeBucket = new NodeBucket(nodeId, executionNodes.indexOf(nodeId));
            nodeBuckets.put(nodeIdIndex, nodeBucket);

        }
        Row partitionRow = partitionedByRow(index);
        nodeBucket.add(inputCursor++, docId, partitionRow, row);
        if (bulkSize != NO_BULK_REQUESTS && nodeBucket.size() >= bulkSize) {
            flushNodeBucket(nodeBucket);
            nodeBuckets.remove(nodeIdIndex);
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
        if (remainingUpstreams.decrementAndGet() == 0) {
            // flush all remaining buckets
            Iterator<NodeBucket> it = nodeBuckets.values().iterator();
            while (it.hasNext()) {
                flushNodeBucket(it.next());
                it.remove();
            }

            closeContexts();

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

    @Nullable
    private Row partitionedByRow(String index) {
        synchronized (partitionRowsCacheLock) {
            if (partitionRowsCache.containsKey(index)) {
                return partitionRowsCache.get(index);
            }
        }
        Row partitionValuesRow = null;
        if (!partitionedBy.isEmpty() && PartitionName.isPartition(index)) {
            Object[] partitionValues;
            List<BytesRef> partitionRowValues = PartitionName.fromStringSafe(index).values();
            partitionValues = new Object[partitionRowValues.size()];
            for (int i = 0; i < partitionRowValues.size(); i++) {
                partitionValues[i] = partitionedBy.get(i).type().value(partitionRowValues.get(i));
            }
            partitionValuesRow = new RowN(partitionValues);
        }
        synchronized (partitionRowsCacheLock) {
            partitionRowsCache.put(index, partitionValuesRow);
        }
        return partitionValuesRow;
    }

    private void flushNodeBucket(final NodeBucket nodeBucket) {
        // every request must increase downstream upstream counter
        downstream.registerUpstream(this);
        remainingRequests.incrementAndGet();

        NodeFetchRequest request = new NodeFetchRequest();
        request.jobId(jobId);
        request.executionNodeId(executionNodeId);
        request.toFetchReferences(toFetchReferences);
        request.jobSearchContextDocIds(nodeBucket.docIds());
        if (bulkSize > NO_BULK_REQUESTS && remainingUpstreams.get() > 0) {
            request.closeContext(false);
        } else {
            nodesWithOpenContext.remove(nodeBucket.nodeId);
        }
        transportFetchNodeAction.execute(nodeBucket.nodeId, request, new ActionListener<NodeFetchResponse>() {
            @Override
            public void onResponse(NodeFetchResponse response) {
                List<Row> rows = new ArrayList<>(response.rows().size());
                int idx = 0;
                synchronized (rowDelegateLock) {
                    for (Row row : response.rows()) {
                        fetchRowDelegate.delegate(row);
                        if (needInputRow) {
                            collectRowDelegate.delegate(nodeBucket.inputRow(idx));
                        }
                        Row partitionRow = nodeBucket.partitionRow(idx);
                        if (partitionRow != null) {
                            partitionRowDelegate.delegate(partitionRow);
                        }
                        try {
                            rows.add(new PositionalRowDelegate(outputRow, nodeBucket.cursor(idx)));
                        } catch (Throwable e) {
                            onFailure(e);
                            return;
                        }
                        idx++;
                    }
                }
                if (!downstream.setNextBucket(rows, nodeBucket.nodeIdx)) {
                    consumingRows.set(false);
                }
                remainingRequests.decrementAndGet();
                downstream.finish();
            }

            @Override
            public void onFailure(Throwable e) {
                if (consumingRows.compareAndSet(true, false)) {
                    downstream.fail(e);
                }
            }
        });

    }

    /**
     * close job contexts on all affected nodes, just fire & forget, they will timeout anyway
     */
    private void closeContexts() {
        LOGGER.trace("closing job context {} on {} nodes", jobId, nodesWithOpenContext.size());
        for (final String nodeId : nodesWithOpenContext) {
            transportCloseContextNodeAction.execute(nodeId,
                    new NodeCloseContextRequest(jobId, executionNodeId),
                    new ActionListener<NodeCloseContextResponse>() {
                @Override
                public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                }

                @Override
                public void onFailure(Throwable e) {
                    LOGGER.warn("Closing job context {} failed on node {} with: {}", e, jobId, nodeId, e.getMessage());
                }
            });
        }
    }

    private static class NodeBucket {

        private final int nodeIdx;
        private final String nodeId;
        private final List<Row> partitionRows = new ArrayList<>();
        private final List<Row> inputRows = new ArrayList<>();
        private final IntArrayList cursors = new IntArrayList();
        private final LongArrayList docIds = new LongArrayList();

        public NodeBucket(String nodeId, int nodeIdx) {
            this.nodeId = nodeId;
            this.nodeIdx = nodeIdx;
        }

        public void add(int cursor, Long docId, @Nullable Row partitionRow, Row row) {
            cursors.add(cursor);
            docIds.add(docId);
            partitionRows.add(partitionRow);
            inputRows.add(new RowN(row.materialize()));
        }

        public int size() {
            return cursors.size();
        }

        public LongArrayList docIds() {
            return docIds;
        }

        public int cursor(int index) {
            return cursors.get(index);
        }

        public Row inputRow(int index) {
            return inputRows.get(index);
        }

        @Nullable
        public Row partitionRow(int idx) {
            return partitionRows.get(idx);
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

        @Override
        public Object[] materialize() {
            return delegate.materialize();
        }
    }

}
