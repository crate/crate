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

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Iterables;
import io.crate.Streamer;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.StreamBucket;
import io.crate.executor.transport.TransportFetchNodeAction;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.fetch.FetchRowInputSymbolVisitor;
import io.crate.planner.node.fetch.FetchSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FetchProjector extends AbstractProjector {

    enum Stage {
        INIT,
        COLLECT,
        FETCH,
        FINALIZE
    }

    private final AtomicReference<Stage> stage = new AtomicReference<>(Stage.INIT);
    private final Object failureLock = new Object();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final FetchRowInputSymbolVisitor.Context collectRowContext;
    private final TransportFetchNodeAction transportFetchNodeAction;

    // TODO: add an estimate to the constructor
    private final ArrayList<Object[]> inputValues = new ArrayList<>();

    private final Map<String, IntSet> nodeReaders;

    private final ThreadPool threadPool;
    private final UUID jobId;
    private final int collectPhaseId;
    private final Map<TableIdent, FetchSource> fetchSources;
    private final TreeMap<Integer, String> readerIndices;
    private final Row outputRow;
    private final AtomicInteger remainingRequests = new AtomicInteger(0);

    private static final ESLogger LOGGER = Loggers.getLogger(FetchProjector.class);
    private Fetches fetches;

    /**
     * An array backed row, which returns the inner array upon materialize
     */
    public static class ArrayBackedRow implements Row {

        private Object[] cells;

        @Override
        public int size() {
            return cells.length;
        }

        @Override
        public Object get(int index) {
            assert cells != null;
            return cells[index];
        }

        @Override
        public Object[] materialize() {
            return cells;
        }
    }

    public FetchProjector(TransportFetchNodeAction transportFetchNodeAction,
                          ThreadPool threadPool,
                          Functions functions,
                          UUID jobId,
                          int collectPhaseId,
                          Map<TableIdent, FetchSource> fetchSources,
                          List<Symbol> outputSymbols,
                          Map<String, IntSet> nodeReaders,
                          TreeMap<Integer, String> readerIndices) {
        this.transportFetchNodeAction = transportFetchNodeAction;
        this.threadPool = threadPool;
        this.jobId = jobId;
        this.collectPhaseId = collectPhaseId;
        this.fetchSources = fetchSources;
        this.nodeReaders = nodeReaders;
        this.readerIndices = readerIndices;

        FetchRowInputSymbolVisitor rowInputSymbolVisitor = new FetchRowInputSymbolVisitor(functions);

        this.collectRowContext = new FetchRowInputSymbolVisitor.Context(fetchSources);

        List<Input<?>> inputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            inputs.add(rowInputSymbolVisitor.process(symbol, collectRowContext));
        }

        outputRow = new InputRow(inputs);
    }

    private boolean nextStage(Stage from, Stage to) {
        synchronized (failureLock) {
            if (failIfNeeded()) return true;
            Stage was = stage.getAndSet(to);
            assert was == from : "wrong state switch " + from + "/" + to + " was " + was;
        }
        return false;
    }

    @Override
    public void prepare(ExecutionState executionState) {
        assert stage.get() == Stage.INIT;
        this.executionState = executionState;
        fetches = new Fetches();
        nextStage(Stage.INIT, Stage.COLLECT);
    }

    @Override
    public boolean setNextRow(Row row) {
        Object[] cells = row.materialize();
        collectRowContext.inputRow().cells = cells;
        for (int i : collectRowContext.docIdPositions()) {
            fetches.require((Long) cells[i]);
        }
        inputValues.add(cells);
        return true;
    }

    private void sendRequests() {
        synchronized (failureLock) {
            remainingRequests.set(nodeReaders.size());
        }
        boolean anyRequestSent = false;
        for (Map.Entry<String, IntSet> entry : nodeReaders.entrySet()) {
            //requests.put(entry.getKey(), request);
            IntObjectOpenHashMap<IntContainer> toFetch = new IntObjectOpenHashMap<>(entry.getValue().size());
            IntObjectOpenHashMap<Streamer[]> streamers = new IntObjectOpenHashMap<>(entry.getValue().size());
            boolean requestRequired = false;
            for (IntCursor intCursor : entry.getValue()) {
                ReaderBucket readerBucket = fetches.readerBucket(intCursor.value);
                IndexInfo indexInfo;
                if (readerBucket == null) {
                    indexInfo = fetches.indexInfo(intCursor.value);
                } else {
                    indexInfo = readerBucket.indexInfo;
                    if (indexInfo.fetchRequired() && readerBucket.docs.size() > 0) {
                        toFetch.put(intCursor.value, readerBucket.docs.keys());
                        streamers.put(intCursor.value, readerBucket.indexInfo.streamers());
                    }
                }
                requestRequired = requestRequired || indexInfo.fetchRequired();
            }
            if (!requestRequired) {
                remainingRequests.decrementAndGet();
                continue;
            }
            NodeFetchRequest request = new NodeFetchRequest(jobId, collectPhaseId, toFetch);
            final String nodeId = entry.getKey();
            anyRequestSent = true;
            transportFetchNodeAction.execute(nodeId, streamers, request, new ActionListener<NodeFetchResponse>() {
                @Override
                public void onResponse(NodeFetchResponse nodeFetchResponse) {
                    IntObjectMap<StreamBucket> fetched = nodeFetchResponse.fetched();
                    if (fetched != null) {
                        for (IntObjectCursor<StreamBucket> cursor : fetched) {
                            ReaderBucket readerBucket = fetches.readerBuckets.get(cursor.key);
                            readerBucket.fetched(cursor.value);
                        }
                    }
                    if (remainingRequests.decrementAndGet() == 0) {
                        Executor executor = threadPool.executor(ThreadPool.Names.SUGGEST);
                        executor.execute(new AbstractRunnable() {
                            @Override
                            public void onFailure(Throwable t) {
                                fail(t);
                            }

                            @Override
                            protected void doRun() throws Exception {
                                fetchFinished();
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    LOGGER.error("NodeFetchRequest failed on node {}", e, nodeId);
                    remainingRequests.decrementAndGet();
                    fail(e);
                }
            });
        }
        if (!anyRequestSent) {
            fetchFinished();
        }
    }

    private void fetchFinished() {
        if (nextStage(Stage.FETCH, Stage.FINALIZE)) {
            return;
        }
        final ArrayBackedRow inputRow = collectRowContext.inputRow();
        final ArrayBackedRow[] fetchRows = collectRowContext.fetchRows();
        final ArrayBackedRow[] partitionRows = collectRowContext.partitionRows();
        final int[] docIdPositions = collectRowContext.docIdPositions();

        for (Object[] cells : inputValues) {
            inputRow.cells = cells;
            for (int i = 0; i < docIdPositions.length; i++) {
                Long doc = (Long) cells[docIdPositions[i]];
                int readerId = (int) (doc >> 32);
                int docId = (int) (long) doc;
                ReaderBucket readerBucket = fetches.readerBuckets.get(readerId);
                assert readerBucket != null;
                // TODO: could be improved by handling non partitioned requests differently
                if (partitionRows != null && partitionRows[i] != null) {
                    assert readerBucket.indexInfo.partitionValues != null;
                    partitionRows[i].cells = readerBucket.indexInfo.partitionValues;
                }
                fetchRows[i].cells = readerBucket.get(docId);
                assert !readerBucket.indexInfo.fetchRequired() || fetchRows[i].cells != null;
            }
            downstream.setNextRow(outputRow);
        }
        finishDownstream();
    }

    @Override
    public void finish() {
        if (nextStage(Stage.COLLECT, Stage.FETCH)) {
            return;
        }
        sendRequests();
    }

    private boolean failIfNeeded() {
        Throwable t = failure.get();
        if (t != null) {
            downstream.fail(t);
            return true;
        }
        return false;
    }

    private void finishDownstream() {
        if (failIfNeeded()) {
            return;
        }
        downstream.finish();
    }

    @Override
    public void fail(Throwable throwable) {
        synchronized (failureLock) {
            boolean first = failure.compareAndSet(null, throwable);
            switch (stage.get()) {
                case INIT:
                    throw new IllegalStateException("Shouldn't call fail on projection if projection hasn't been prepared");
                case COLLECT:
                    if (first) {
                        sendRequests();
                        return;
                    }
                case FETCH:
                    if (remainingRequests.get() > 0) return;
            }
        }
        downstream.fail(throwable);
    }

    private static class IndexInfo {
        final FetchSource fetchSource;
        Object[] partitionValues;
        Streamer[] streamers;

        public IndexInfo(String index, FetchSource fetchSource) {
            this.fetchSource = fetchSource;
            if (!fetchSource.partitionedByColumns().isEmpty()) {
                PartitionName pn = PartitionName.fromIndexOrTemplate(index);
                setPartitionValues(pn);
            }
        }

        private void setPartitionValues(PartitionName pn) {
            List<BytesRef> partitionRowValues = pn.values();
            partitionValues = new Object[partitionRowValues.size()];
            for (int i = 0; i < partitionRowValues.size(); i++) {
                partitionValues[i] = fetchSource.partitionedByColumns().get(i).type().value(partitionRowValues.get(i));
            }
        }

        public boolean fetchRequired() {
            return !fetchSource.references().isEmpty();
        }

        public Streamer[] streamers() {
            if (streamers == null) {
                streamers = Symbols.streamerArray(fetchSource.references());
            }
            return streamers;
        }
    }

    private FetchSource getFetchSource(String index) {
        if (fetchSources.size() == 1) {
            return Iterables.getOnlyElement(fetchSources.values());
        }
        TableIdent ti = TableIdent.fromIndexName(index);
        return fetchSources.get(ti);
    }

    private class Fetches {
        private final IntObjectOpenHashMap<ReaderBucket> readerBuckets = new IntObjectOpenHashMap<>();
        private final TreeMap<Integer, IndexInfo> indexInfos;

        private Fetches() {
            this.indexInfos = new TreeMap<>();
            for (Map.Entry<Integer, String> entry : readerIndices.entrySet()) {
                indexInfos.put(entry.getKey(), new IndexInfo(entry.getValue(), getFetchSource(entry.getValue())));
            }
        }

        public IndexInfo indexInfo(Integer readerId) {
            return indexInfos.floorEntry(readerId).getValue();
        }

        public ReaderBucket readerBucket(int readerId) {
            return readerBuckets.get(readerId);
        }

        public ReaderBucket require(long doc) {
            int readerId = (int) (doc >> 32);
            int docId = (int) doc;
            ReaderBucket readerBucket = readerBuckets.get(readerId);
            if (readerBucket == null) {
                readerBucket = new ReaderBucket(indexInfo(readerId));
                readerBuckets.put(readerId, readerBucket);
            }
            readerBucket.require(docId);
            return readerBucket;
        }
    }

    public static class ReaderBucket {

        private final IndexInfo indexInfo;
        private final IntObjectOpenHashMap<Object[]> docs = new IntObjectOpenHashMap<>();

        public ReaderBucket(IndexInfo indexInfo) {
            this.indexInfo = indexInfo;
        }

        public void require(int doc) {
            docs.putIfAbsent(doc, null);
        }

        public Object[] get(int doc) {
            return docs.get(doc);
        }

        public void fetched(Bucket bucket) {
            assert bucket.size() == docs.size();
            Iterator<Row> rowIterator = bucket.iterator();
            final Object[] values = docs.values;
            final boolean[] states = docs.allocated;
            for (int i = 0; i < states.length; i++) {
                if (states[i]) {
                    assert values[i] == null;
                    values[i] = rowIterator.next().materialize();
                }
            }
            assert !rowIterator.hasNext();
        }
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }
}
