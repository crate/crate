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
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.*;
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
    private final CollectExpression<?> collectDocIdExpression;
    private final List<ReferenceInfo> partitionedBy;
    private final List<Reference> toFetchReferences;
    private final IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private final IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard;
    private final int bulkSize;
    private final boolean closeContexts;
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
    private final Map<String, Object[]> partitionValuesCache = new HashMap<>();
    private final Object partitionValuesCacheLock = new Object();

    private int inputCursor = 0;
    private boolean consumedRows = false;
    private boolean needInputRow = false;

    private static final ESLogger LOGGER = Loggers.getLogger(FetchProjector.class);

    public FetchProjector(TransportFetchNodeAction transportFetchNodeAction,
                          TransportCloseContextNodeAction transportCloseContextNodeAction,
                          Functions functions,
                          UUID jobId,
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
        this.collectDocIdExpression = collectDocIdExpression;
        this.partitionedBy = partitionedBy;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.jobSearchContextIdToShard = jobSearchContextIdToShard;
        this.bulkSize = bulkSize;
        this.closeContexts = closeContexts;
        numNodes = executionNodes.size();
        this.executionNodes = new ArrayList<>(executionNodes);

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
        consumedRows = true;
        collectDocIdExpression.setNextRow(row);

        long docId = (Long)collectDocIdExpression.value();
        int jobSearchContextId = (int)(docId >> 32);

        String nodeId = jobSearchContextIdToNode.get(jobSearchContextId);
        String index = jobSearchContextIdToShard.get(jobSearchContextId).getIndex();
        int nodeIdIndex = Objects.hash(nodeId, index);

        NodeBucket nodeBucket = nodeBuckets.get(nodeIdIndex);
        if (nodeBucket == null) {
            nodeBucket = new NodeBucket(nodeId, jobSearchContextIdToShard.get(jobSearchContextId).getIndex(), executionNodes.indexOf(nodeId));
            nodeBuckets.put(nodeIdIndex, nodeBucket);

        }
        nodeBucket.add(inputCursor++, docId, row);
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

            // projector registered itself as an upstream to prevent downstream of
            // flushing rows before all requests finished.
            // release it now as no new rows are consumed anymore (downstream will flush all remaining rows)
            downstream.finish();

            // no rows consumed (so no fetch requests made), but collect contexts are open, close them.
            if (!consumedRows) {
                closeContexts();
            }
        }
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }

    @Nullable
    private Row partitionedByRow(String index) {
        if (!partitionedBy.isEmpty() && PartitionName.isPartition(index)) {
            Object[] partitionValues;
            synchronized (partitionValuesCacheLock) {
                partitionValues = partitionValuesCache.get(index);
                if (partitionValues == null) {
                    List<BytesRef> partitionRawValues = PartitionName.fromStringSafe(index).values();
                    partitionValues = new Object[partitionRawValues.size()];
                    for (int i = 0; i < partitionRawValues.size(); i++) {
                        partitionValues[i] = partitionedBy.get(i).type().value(partitionRawValues.get(i));
                    }
                    partitionValuesCache.put(index, partitionValues);
                }
            }
            return new RowN(partitionValues);
        }
        return null;
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
        final Row partitionRow = partitionedByRow(nodeBucket.index);
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
                if (remainingRequests.decrementAndGet() <= 0 && remainingUpstreams.get() <= 0) {
                    closeContexts();
                }
                downstream.finish();
            }

            @Override
            public void onFailure(Throwable e) {
                consumingRows.set(false);
                downstream.fail(e);
            }
        });

    }

    /**
     * close job contexts on all affected nodes, just fire & forget, they will timeout anyway
     */
    private void closeContexts() {
        if (closeContexts || bulkSize > NO_BULK_REQUESTS) {
            LOGGER.trace("closing job context {} on {} nodes", jobId, numNodes);
            for (final String nodeId : executionNodes) {
                transportCloseContextNodeAction.execute(nodeId, new NodeCloseContextRequest(jobId), new ActionListener<NodeCloseContextResponse>() {
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
    }

    private static class NodeBucket {

        private final int nodeIdx;
        private final String nodeId;
        private final String index;
        private final List<Row> inputRows = new ArrayList<>();
        private final IntArrayList cursors = new IntArrayList();
        private final LongArrayList docIds = new LongArrayList();

        public NodeBucket(String nodeId, String index, int nodeIdx) {
            this.nodeId = nodeId;
            this.index = index;
            this.nodeIdx = nodeIdx;
        }

        public void add(int cursor, Long docId, Row row) {
            cursors.add(cursor);
            docIds.add(docId);
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
