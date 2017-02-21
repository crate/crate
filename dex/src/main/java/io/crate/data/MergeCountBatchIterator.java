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

public class MergeCountBatchIterator implements BatchIterator {

    private final BatchIterator source;

    private CompletableFuture<Row> loading = null;
    private Row result = null;
    private Row currentRow = OFF_ROW;
    private long sum = 0;

    public static BatchIterator newInstance(BatchIterator source) {
        return new CloseAssertingBatchIterator(new MergeCountBatchIterator(source));
    }

    private MergeCountBatchIterator(BatchIterator source) {
        this.source = source;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        currentRow = OFF_ROW;

        if (loading == null) {
            source.moveToStart();
        } else {
            result = loading.join();
        }
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        if (result == null) {
            currentRow = OFF_ROW;
            return false;
        }
        currentRow = result;
        result = null;
        return true;
    }

    @Override
    public Row currentRow() {
        raiseIfLoading();
        return currentRow;
    }

    @Override
    public void close() {
        source.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (loading == null) {
            loading = BatchRowVisitor.visitRows(source, this::onRow)
                .thenApply(v -> {
                    result = new Row1(sum);
                    return result;
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
