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

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.analyze.symbol.Symbol;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchAccumulator;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.Functions;
import io.crate.operation.InputRow;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

public class FetchBatchAccumulator implements BatchAccumulator<Row, Iterator<? extends Row>> {

    private static final Logger LOGGER = Loggers.getLogger(FetchBatchAccumulator.class);

    private final FetchOperation fetchOperation;
    private final FetchProjectorContext context;
    private final int fetchSize;
    private final FetchRowInputSymbolVisitor.Context collectRowContext;
    private final InputRow outputRow;
    private final ArrayList<Object[]> inputValues = new ArrayList<>();

    public FetchBatchAccumulator(FetchOperation fetchOperation,
                                 Functions functions,
                                 List<Symbol> outputSymbols,
                                 FetchProjectorContext fetchProjectorContext,
                                 int fetchSize) {
        this.fetchOperation = fetchOperation;
        this.context = fetchProjectorContext;
        this.fetchSize = fetchSize;

        FetchRowInputSymbolVisitor rowInputSymbolVisitor = new FetchRowInputSymbolVisitor(functions);
        this.collectRowContext = new FetchRowInputSymbolVisitor.Context(fetchProjectorContext.tableToFetchSource);

        List<Input<?>> inputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            inputs.add(rowInputSymbolVisitor.process(symbol, collectRowContext));
        }
        outputRow = new InputRow(inputs);
    }

    @Override
    public void onItem(Row row) {
        Object[] cells = row.materialize();
        collectRowContext.inputRow().cells = cells;
        for (int i : collectRowContext.fetchIdPositions()) {
            Object fetchId = cells[i];
            if (fetchId != null) {
                context.require((long) fetchId);
            }
        }
        inputValues.add(cells);
    }

    @Override
    public CompletableFuture<Iterator<? extends Row>> processBatch(boolean isLastBatch) {
        List<CompletableFuture<IntObjectMap<? extends Bucket>>> futures = new ArrayList<>();
        Iterator<Map.Entry<String, IntSet>> it = context.nodeToReaderIds.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, IntSet> entry = it.next();
            IntObjectHashMap<IntContainer> toFetch = generateToFetch(entry.getValue());
            if (toFetch.isEmpty() && !isLastBatch) {
                continue;
            }
            final String nodeId = entry.getKey();
            try {
                futures.add(fetchOperation.fetch(nodeId, toFetch, isLastBatch));
            } catch (Throwable t) {
                futures.add(CompletableFutures.failedFuture(t));
            }
            if (isLastBatch) {
                it.remove();
            }
        }
        return CompletableFutures.allAsList(futures).thenApply(this::getRows);
    }

    @Override
    public void close() {
        for (String nodeId : context.nodeToReaderIds.keySet()) {
            fetchOperation.fetch(nodeId, new IntObjectHashMap<>(0), true)
                .exceptionally(e -> {
                    LOGGER.error("Error sending close fetchRequest to node={}", e, nodeId);
                    return null;
                });
        }
    }

    @Override
    public void reset() {
        context.clearBuckets();
        inputValues.clear();
    }

    private Iterator<? extends Row> getRows(List<IntObjectMap<? extends Bucket>> results) {
        applyResultToReaderBuckets(results);
        return new Iterator<Row>() {

            final int[] fetchIdPositions = collectRowContext.fetchIdPositions();
            final ArrayBackedRow inputRow = collectRowContext.inputRow();
            final ArrayBackedRow[] fetchRows = collectRowContext.fetchRows();
            final ArrayBackedRow[] partitionRows = collectRowContext.partitionRows();
            final Object[][] nullCells = collectRowContext.nullCells();

            int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < inputValues.size();
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator is exhausted");
                }
                Object[] cells = inputValues.get(idx);
                inputRow.cells = cells;
                for (int i = 0; i < fetchIdPositions.length; i++) {
                    Object fetchIdObj = cells[fetchIdPositions[i]];
                    if (fetchIdObj == null) {
                        fetchRows[i].cells = nullCells[i];
                        continue;
                    }
                    long fetchId = (long) fetchIdObj;
                    int readerId = FetchId.decodeReaderId(fetchId);
                    int docId = FetchId.decodeDocId(fetchId);
                    ReaderBucket readerBucket = context.getReaderBucket(readerId);
                    assert readerBucket != null : "readerBucket must not be null";
                    setPartitionRow(partitionRows, i, readerBucket);
                    fetchRows[i].cells = readerBucket.get(docId);
                    assert !readerBucket.fetchRequired() || fetchRows[i].cells != null :
                        "readerBucket doesn't require fetch or row is fetched";
                }
                idx++;
                if (!hasNext()) {
                    // free up memory - in case we're streaming data to the client
                    // this would otherwise grow to hold the whole result in-memory
                    reset();
                }
                return outputRow;
            }
        };
    }

    @Override
    public int batchSize() {
        return fetchSize;
    }

    private void applyResultToReaderBuckets(List<IntObjectMap<? extends Bucket>> results) {
        for (IntObjectMap<? extends Bucket> result : results) {
            if (result == null) {
                continue;
            }
            for (IntObjectCursor<? extends Bucket> cursor : result) {
                ReaderBucket readerBucket = context.getReaderBucket(cursor.key);
                readerBucket.fetched(cursor.value);
            }
        }
    }

    private IntObjectHashMap<IntContainer> generateToFetch(IntSet readerIds) {
        IntObjectHashMap<IntContainer> toFetch = new IntObjectHashMap<>(readerIds.size());
        for (IntCursor readerIdCursor : readerIds) {
            ReaderBucket readerBucket = context.readerBucket(readerIdCursor.value);
            if (readerBucket != null && readerBucket.fetchRequired() && readerBucket.docs.size() > 0) {
                toFetch.put(readerIdCursor.value, readerBucket.docs.keys());
            }
        }
        return toFetch;
    }

    private void setPartitionRow(ArrayBackedRow[] partitionRows, int i, ReaderBucket readerBucket) {
        if (partitionRows != null && partitionRows[i] != null) {
            assert readerBucket.partitionValues != null : "readerBucket's partitionValues must not be null";
            partitionRows[i].cells = readerBucket.partitionValues;
        }
    }
}
