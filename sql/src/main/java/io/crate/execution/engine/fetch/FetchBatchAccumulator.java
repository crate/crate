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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchAccumulator;
import io.crate.data.Bucket;
import io.crate.data.Row;

public class FetchBatchAccumulator implements BatchAccumulator<Row, Iterator<? extends Row>> {

    private static final Logger LOGGER = LogManager.getLogger(FetchBatchAccumulator.class);

    private final FetchOperation fetchOperation;
    private final ReaderBuckets readerBuckets;
    private final int fetchSize;
    private final ArrayList<Object[]> inputValues = new ArrayList<>();
    private final FetchRows fetchRows;
    private final Map<String, IntSet> readerIdsByNode;

    public FetchBatchAccumulator(FetchRows fetchRows,
                                 FetchOperation fetchOperation,
                                 Map<String, IntSet> readerIdsByNode,
                                 int fetchSize) {
        this.fetchOperation = fetchOperation;
        this.readerIdsByNode = readerIdsByNode;
        this.readerBuckets = new ReaderBuckets();
        this.fetchSize = fetchSize;
        this.fetchRows = fetchRows;
    }

    @Override
    public void onItem(Row row) {
        Object[] cells = row.materialize();
        for (int i : fetchRows.fetchIdPositions()) {
            Object fetchId = cells[i];
            if (fetchId != null) {
                readerBuckets.require((long) fetchId);
            }
        }
        inputValues.add(cells);
    }

    @Override
    public CompletableFuture<Iterator<? extends Row>> processBatch(boolean isLastBatch) {
        List<CompletableFuture<IntObjectMap<? extends Bucket>>> futures = new ArrayList<>();
        Iterator<Map.Entry<String, IntSet>> it = readerIdsByNode.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, IntSet> entry = it.next();
            IntObjectHashMap<IntContainer> toFetch = readerBuckets.generateToFetch(entry.getValue());
            if (toFetch.isEmpty() && !isLastBatch) {
                continue;
            }
            final String nodeId = entry.getKey();
            try {
                futures.add(fetchOperation.fetch(nodeId, toFetch, isLastBatch));
            } catch (Throwable t) {
                futures.add(CompletableFuture.failedFuture(t));
            }
            if (isLastBatch) {
                it.remove();
            }
        }
        return CompletableFutures.allAsList(futures).thenApply(this::getRows);
    }

    @Override
    public void close() {
        for (String nodeId : readerIdsByNode.keySet()) {
            fetchOperation.fetch(nodeId, new IntObjectHashMap<>(0), true)
                .exceptionally(e -> {
                    LOGGER.error("An error happened while sending close fetchRequest to node=" + nodeId, e);
                    return null;
                });
        }
    }

    @Override
    public void reset() {
        readerBuckets.clearBuckets();
        inputValues.clear();
    }

    private Iterator<? extends Row> getRows(List<IntObjectMap<? extends Bucket>> results) {
        readerBuckets.applyResults(results);
        return new Iterator<Row>() {

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
                idx++;
                Row outputRow = fetchRows.updatedOutputRow(cells, readerBuckets);
                if (!hasNext()) {
                    readerBuckets.clearBuckets();
                }
                return outputRow;
            }
        };
    }

    @Override
    public int batchSize() {
        return fetchSize;
    }
}
