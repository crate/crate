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

package io.crate.data;

import com.google.common.annotations.VisibleForTesting;
import io.crate.concurrent.CompletableFutures;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation that is backed by {@link Iterable<Row>}.
 *
 */
public class RowsBatchIterator implements BatchIterator {

    private final RowColumns rowData;
    private final Iterable<? extends Row> rows;

    private Iterator<? extends Row> it;

    public static BatchIterator empty() {
        return newInstance(Collections.emptyList(), 0);
    }

    public static BatchIterator newInstance(Row row) {
        return newInstance(Collections.singletonList(row), row.numColumns());
    }

    public static BatchIterator newInstance(Iterable<? extends Row> rows, int numCols) {
        return new CloseAssertingBatchIterator(new RowsBatchIterator(rows, numCols));
    }

    @VisibleForTesting
    RowsBatchIterator(Iterable<? extends Row> rows, int numCols) {
        rowData = new RowColumns(numCols);
        this.rows = rows;
        this.it = rows.iterator();
    }

    @Override
    public Columns rowData() {
        return rowData;
    }

    @Override
    public void moveToStart() {
        it = rows.iterator();
        rowData.updateRef(RowBridging.OFF_ROW);
    }

    @Override
    public boolean moveNext() {
        if (it.hasNext()) {
            rowData.updateRef(it.next());
            return true;
        }
        rowData.updateRef(RowBridging.OFF_ROW);
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return CompletableFutures.failedFuture(new IllegalStateException("All batches already loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

}
