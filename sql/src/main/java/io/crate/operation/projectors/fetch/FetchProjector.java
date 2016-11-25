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

package io.crate.operation.projectors.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.fetch.FetchRowInputSymbolVisitor;
import io.crate.operation.projectors.*;
import io.crate.operation.reference.doc.lucene.FetchIds;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FetchProjector extends AbstractProjector {

    private final int fetchSize;
    private final FetchProjectorContext context;
    private final FetchOperation fetchOperation;
    private final AtomicBoolean finishCalled = new AtomicBoolean(false);
    private final AtomicInteger resumeLatch = new AtomicInteger(2);
    private int currentRowCount = 0;
    private ResumeHandle resumeHandle = ResumeHandle.INVALID;

    enum Stage {
        COLLECT,
        FETCH,
        EMIT,
        FINALIZE
    }

    private final AtomicReference<Stage> stage = new AtomicReference<>(Stage.COLLECT);
    private final Object failureLock = new Object();

    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final FetchRowInputSymbolVisitor.Context collectRowContext;

    // TODO: add an estimate to the constructor
    private final ArrayList<Object[]> inputValues = new ArrayList<>();
    private final Executor resultExecutor;

    private final Row outputRow;
    private final AtomicInteger remainingRequests = new AtomicInteger(0);

    private static final ESLogger LOGGER = Loggers.getLogger(FetchProjector.class);

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

    public FetchProjector(FetchOperation fetchOperation,
                          Executor resultExecutor,
                          Functions functions,
                          List<Symbol> outputSymbols,
                          FetchProjectorContext fetchProjectorContext,
                          int fetchSize) {
        this.fetchSize = fetchSize;

        this.fetchOperation = fetchOperation;
        this.context = fetchProjectorContext;
        this.resultExecutor = resultExecutor;

        FetchRowInputSymbolVisitor rowInputSymbolVisitor = new FetchRowInputSymbolVisitor(functions);
        this.collectRowContext = new FetchRowInputSymbolVisitor.Context(fetchProjectorContext.tableToFetchSource);

        List<Input<?>> inputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            inputs.add(rowInputSymbolVisitor.process(symbol, collectRowContext));
        }
        outputRow = new InputRow(inputs);
    }

    private boolean nextStage(Stage from, Stage to) {
        LOGGER.trace("Changing state from {} to {}", from, to);
        synchronized (failureLock) {
            if (failIfNeeded()) return true;
            Stage was = stage.getAndSet(to);
            assert was == from : "wrong state switch " + from + "/" + to + " was " + was;
        }
        return false;
    }

    @Override
    public Result setNextRow(Row row) {
        Object[] cells = row.materialize();
        collectRowContext.inputRow().cells = cells;
        for (int i : collectRowContext.fetchIdPositions()) {
            Object fetchId = cells[i];
            if (fetchId != null) {
                context.require((long) fetchId);
            }
        }
        inputValues.add(cells);

        // Check if fetchSize is reached and dispatch fetch requests
        currentRowCount++;
        if (fetchSize > 0 && currentRowCount % fetchSize == 0) {
            sendRequests(false);
            return Result.PAUSE;
        }
        return Result.CONTINUE;
    }

    private void sendRequests(final boolean isLast) {
        resumeLatch.set(2);
        if (nextStage(Stage.COLLECT, Stage.FETCH)) {
            return;
        }
        if (context.nodeToReaderIds.isEmpty()) {
            sendToDownstream(true, 0);
            return;
        }
        synchronized (failureLock) {
            remainingRequests.set(context.nodeToReaderIds.size());
        }
        for (Map.Entry<String, IntSet> entry : context.nodeToReaderIds.entrySet()) {
            IntObjectHashMap<IntContainer> toFetch = generateToFetch(entry);
            if (toFetch.isEmpty() && !isLast) {
                if (remainingRequests.decrementAndGet() == 0) {
                    sendToDownstream(false, 0);
                }
            } else {
                final String nodeId = entry.getKey();
                ListenableFuture<IntObjectMap<? extends Bucket>> future = fetchOperation.fetch(nodeId, toFetch, isLast);

                Futures.addCallback(future, new FutureCallback<IntObjectMap<? extends Bucket>>() {
                    @Override
                    public void onSuccess(@Nullable IntObjectMap<? extends Bucket> result) {
                        if (result != null) {
                            for (IntObjectCursor<? extends Bucket> cursor : result) {
                                ReaderBucket readerBucket = context.getReaderBucket(cursor.key);
                                readerBucket.fetched(cursor.value);
                            }
                        }
                        if (remainingRequests.decrementAndGet() == 0) {
                            resultExecutor.execute(new SendToDownstreamRunnable(isLast));
                        }
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        LOGGER.error("NodeFetchRequest failed on node {}", t, nodeId);
                        remainingRequests.decrementAndGet();
                        fail(t);
                    }
                });
            }
        }
    }

    private IntObjectHashMap<IntContainer> generateToFetch(Map.Entry<String, IntSet> entry) {
        IntObjectHashMap<IntContainer> toFetch = new IntObjectHashMap<>(entry.getValue().size());
        for (IntCursor readerIdCursor : entry.getValue()) {
            ReaderBucket readerBucket = context.readerBucket(readerIdCursor.value);
            if (readerBucket != null && readerBucket.fetchRequired() && readerBucket.docs.size() > 0) {
                toFetch.put(readerIdCursor.value, readerBucket.docs.keys());
            }
        }
        return toFetch;
    }

    private void sendToDownstream(final boolean isLast, final int rowStartIdx) {
        if (rowStartIdx == 0 && nextStage(Stage.FETCH, Stage.EMIT)) {
            return;
        }
        final ArrayBackedRow inputRow = collectRowContext.inputRow();
        final ArrayBackedRow[] fetchRows = collectRowContext.fetchRows();
        final ArrayBackedRow[] partitionRows = collectRowContext.partitionRows();
        final int[] fetchIdPositions = collectRowContext.fetchIdPositions();
        final Object[][] nullCells = collectRowContext.nullCells();

        loop:
        for (int i = rowStartIdx; i < inputValues.size(); i++) {
            Object[] cells = inputValues.get(i);
            inputRow.cells = cells;
            for (int j = 0; j < fetchIdPositions.length; j++) {
                Object fetchObject = cells[fetchIdPositions[j]];
                if (fetchObject == null) {
                    // can be null on outer joins
                    fetchRows[j].cells = nullCells[j];
                    continue;
                }
                long fetchId = (long) fetchObject;
                int readerId = FetchIds.extractReaderId(fetchId);
                int docId = FetchIds.extractDocId(fetchId);
                ReaderBucket readerBucket = context.getReaderBucket(readerId);
                assert readerBucket != null;
                setPartitionRow(partitionRows, j, readerBucket);
                fetchRows[j].cells = readerBucket.get(docId);
                assert !readerBucket.fetchRequired() || fetchRows[j].cells != null;
            }
            Result result = downstream.setNextRow(outputRow);
            switch (result) {
                case CONTINUE:
                    continue;
                case PAUSE:
                    final int startIdx = i + 1;
                    downstream.pauseProcessed(new ResumeHandle() {
                        @Override
                        public void resume(boolean async) {
                            ExecutorResumeHandle.resume(resultExecutor, new Runnable() {
                                @Override
                                public void run() {
                                    sendToDownstream(isLast, startIdx);
                                }
                            }, async);
                        }
                    });
                    return;
                case STOP:
                    break loop;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
        }
        context.clearBuckets();
        if (!isLast) {
            if (nextStage(Stage.EMIT, Stage.COLLECT)) {
                return;
            }
            inputValues.clear();
            currentRowCount = 0;
            resume();
        } else {
            finishDownstream();
        }
    }


    @Override
    public void pauseProcessed(ResumeHandle resumeHandle) {
        if (resumeLatch.decrementAndGet() == 0) {
            resumeHandle.resume(false);
        } else {
            this.resumeHandle = resumeHandle;
        }
    }

    private void resume() {
        if (resumeLatch.decrementAndGet() == 0) {
            ResumeHandle resumeHandle = this.resumeHandle;
            this.resumeHandle = ResumeHandle.INVALID;
            resumeHandle.resume(false);
        }
    }

    private void setPartitionRow(ArrayBackedRow[] partitionRows, int i, ReaderBucket readerBucket) {
        // TODO: could be improved by handling non partitioned requests differently
        if (partitionRows != null && partitionRows[i] != null) {
            assert readerBucket.partitionValues != null;
            partitionRows[i].cells = readerBucket.partitionValues;
        }
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        if (!finishCalled.getAndSet(true)) {
            sendRequests(true);
        }
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
        if (nextStage(Stage.EMIT, Stage.FINALIZE)) {
            return;
        }
        if (failIfNeeded()) {
            return;
        }
        downstream.finish(RepeatHandle.UNSUPPORTED);
    }

    @Override
    public void fail(Throwable throwable) {
        synchronized (failureLock) {
            boolean first = failure.compareAndSet(null, throwable);
            switch (stage.get()) {
                case COLLECT:
                    if (first) {
                        if (!finishCalled.getAndSet(true)) {
                            sendRequests(true);
                        }
                        return;
                    }
                case FETCH:
                    if (remainingRequests.get() > 0) return;
            }
        }
        downstream.fail(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        downstream.kill(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return downstream.requirements();
    }

    private class SendToDownstreamRunnable extends AbstractRunnable {
        private final boolean isLast;

        SendToDownstreamRunnable(boolean isLast) {
            this.isLast = isLast;
        }

        @Override
        public void onFailure(Throwable t) {
            fail(t);
        }

        @Override
        protected void doRun() throws Exception {
            sendToDownstream(isLast, 0);
        }
    }
}
