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

import io.crate.concurrent.CompletableFutures;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A BatchIterator that consumes another BatchIterator summing up the first column (expected to be of type long)
 *
 * <pre>
 *     source BatchIterator:
 *     [ 1, 1, 4, 1 ]
 *
 *     output:
 *     [ 7 ]
 * </pre>
 */
public class SummingBatchIterator implements BatchIterator {

    private final BatchIterator source;

    private CompletableFuture<?> loading = null;
    private Row result = null;
    private Row currentRow = OFF_ROW;
    private long sum = 0;
    private boolean atStart = true;

    public static BatchIterator newInstance(BatchIterator source) {
        return new CloseAssertingBatchIterator(new SummingBatchIterator(source));
    }

    private SummingBatchIterator(BatchIterator source) {
        this.source = source;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        currentRow = OFF_ROW;
        atStart = true;
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        if (result != null && atStart) {
            currentRow = result;
            atStart = false;
            return true;
        }
        currentRow = OFF_ROW;
        return false;
    }

    @Override
    public Row currentRow() {
        raiseIfLoading();
        return currentRow;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (loading == null) {
            loading = BatchRowVisitor.visitRows(source, this::onRow)
                .thenAccept(v -> {
                    source.close();
                    result = new Row1(sum);
                });
            return loading;
        }
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator is already loading"));
    }

    @Override
    public boolean allLoaded() {
        return loading != null;
    }

    private void onRow(Row row) {
        sum += ((long) row.get(0));
    }

    private void raiseIfLoading() {
        if (loading != null && loading.isDone() == false) {
            throw new IllegalStateException("BatchIterator is loading");
        }
    }
}
