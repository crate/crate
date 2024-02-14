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

package io.crate.execution.engine.fetch;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

import org.apache.lucene.util.Accountable;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import io.crate.breaker.CellsSizeEstimator;
import io.crate.data.Bucket;
import io.crate.data.CloseableIterator;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.planner.node.fetch.FetchSource;

/**
 * Stores incoming rows and partitions them based on the _fetchIds in the rows into buckets per readerId.
 *
 * <pre>
 * Example use:
 *
 *  for row in source:
 *      readerBuckets.add(row);
 *
 *  docIdsByReader = readerBuckets.generateToFetch(readerIds);
 *  listOfFetchRowsByReader = fetchOperation(docIdsByReader)
 *  result = readerBuckets.getOutputRows(listOfBucketByReader)
 * </pre>
 */
public class ReaderBuckets implements Accountable {

    private final IntObjectHashMap<ReaderBucket> readerBuckets = new IntObjectHashMap<>();
    private final ArrayList<Object[]> rows = new ArrayList<>();
    private final FetchRows fetchRows;
    private final CellsSizeEstimator estimateRow;
    private final RamAccounting ramAccounting;
    private final IntFunction<FetchSource> getFetchSource;

    private long usedMemoryEstimateInBytes;

    public ReaderBuckets(FetchRows fetchRows,
                         IntFunction<FetchSource> getFetchSource,
                         CellsSizeEstimator estimateRow,
                         RamAccounting ramAccounting) {
        this.fetchRows = fetchRows;
        this.getFetchSource = getFetchSource;
        this.estimateRow = estimateRow;
        this.ramAccounting = ramAccounting;
    }

    public void add(Row row) {
        Object[] cells = row.materialize();
        long size = estimateRow.estimateSize(cells);
        ramAccounting.addBytes(size);
        usedMemoryEstimateInBytes += size;
        rows.add(cells);
        for (int i : fetchRows.fetchIdPositions()) {
            Object fetchId = cells[i];
            if (fetchId == null) {
                continue;
            }
            require((long) fetchId);
        }
    }

    private void require(long fetchId) {
        int readerId = FetchId.decodeReaderId(fetchId);
        int docId = FetchId.decodeDocId(fetchId);
        ReaderBucket readerBucket = readerBuckets.get(readerId);
        if (readerBucket == null) {
            readerBucket = new ReaderBucket(ramAccounting, getFetchSource.apply(readerId));
            readerBuckets.put(readerId, readerBucket);
        }
        readerBucket.require(docId);
    }

    public CloseableIterator<Row> getOutputRows(List<IntObjectMap<? extends Bucket>> resultsByReader) {
        for (IntObjectMap<? extends Bucket> result : resultsByReader) {
            if (result == null) {
                continue;
            }
            for (IntObjectCursor<? extends Bucket> cursor : result) {
                ReaderBucket readerBucket = readerBuckets.get(cursor.key);
                assert readerBucket != null
                    : "If we get a result for a reader, there must be a readerBucket for it";
                usedMemoryEstimateInBytes += readerBucket.fetched(cursor.value);
                cursor.value = null;
            }
        }
        return new CloseableIterator<Row>() {

            int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < rows.size();
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Iterator is exhausted");
                }
                Object[] row = rows.get(idx);
                idx++;
                return fetchRows.updatedOutputRow(row, readerBuckets::get);
            }

            @Override
            public void close() throws RuntimeException {
                rows.clear();
                readerBuckets.release();
                ramAccounting.addBytes(- usedMemoryEstimateInBytes);
                usedMemoryEstimateInBytes = 0;
            }
        };
    }

    public IntObjectHashMap<IntArrayList> generateToFetch(IntSet readerIds) {
        IntObjectHashMap<IntArrayList> toFetch = new IntObjectHashMap<>(readerIds.size());
        for (IntCursor readerIdCursor : readerIds) {
            ReaderBucket readerBucket = readerBuckets.get(readerIdCursor.value);
            if (readerBucket != null && !readerBucket.isEmpty()) {
                toFetch.put(readerIdCursor.value, readerBucket.sortedDocs());
            }
        }
        return toFetch;
    }

    @Override
    public long ramBytesUsed() {
        return usedMemoryEstimateInBytes;
    }
}
