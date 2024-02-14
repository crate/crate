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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.breaker.CellsSizeEstimator;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.node.fetch.FetchSource;

class ReaderBucket {

    private final IntObjectHashMap<Object[]> docs = new IntObjectHashMap<>();
    private final RamAccounting ramAccounting;
    private final CellsSizeEstimator estimateCellsSize;

    private IntArrayList sortedDocs;

    ReaderBucket(RamAccounting ramAccounting, FetchSource fetchSource) {
        this.ramAccounting = ramAccounting;
        this.estimateCellsSize = new CellsSizeEstimator(Symbols.typeView(fetchSource.references()));
    }

    void require(int doc) {
        docs.putIfAbsent(doc, null);
    }

    Object[] get(int doc) {
        return docs.get(doc);
    }

    IntArrayList sortedDocs() {
        // The FetchCollector has an optimization that only works if the documents are sequential
        // We pre-sort the ids here to ensure the optimization could work and also so that we can
        // map the results to the right rows, because the rows in the bucket we receive from the FetchCollector
        // are in the same order as the ids we requested.
        if (sortedDocs == null) {
            int[] keys = docs.keys().toArray();
            Arrays.sort(keys);
            sortedDocs = new IntArrayList(keys.length);
            sortedDocs.add(keys);
        }
        return sortedDocs;
    }

    /**
     * @return the bytes added to the ramAccounting instance
     **/
    long fetched(Bucket bucket) {
        assert bucket.size() == docs.size()
            : String.format(Locale.ENGLISH, "requested %d docs but got %d", docs.size(), bucket.size());
        assert sortedDocs != null : "sortedDocs() must have been called before fetched()";

        Iterator<Row> rowIterator = bucket.iterator();
        long bytesAccounted = 0;
        for (var cursor : sortedDocs) {
            Object[] cells = rowIterator.next().materialize();
            bytesAccounted += accountMemory(cells);
            docs.put(cursor.value, cells);
        }
        sortedDocs = null;
        assert !rowIterator.hasNext() : "no more rows should exist";
        return bytesAccounted;
    }

    private long accountMemory(Object[] cells) {
        long bytes = estimateCellsSize.applyAsLong(cells);
        ramAccounting.addBytes(bytes);
        return bytes;
    }

    public boolean isEmpty() {
        return docs.isEmpty();
    }
}
