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

import java.util.concurrent.CompletionStage;

public class LimitingBatchIterator implements BatchIterator {

    private final BatchIterator delegate;
    private final int limit;

    private int rowCount = 0;
    private Row currentRow = OFF_ROW;

    public static BatchIterator newInstance(BatchIterator delegate, int limit) {
        return new CloseAssertingBatchIterator(new LimitingBatchIterator(delegate, limit));
    }

    private LimitingBatchIterator(BatchIterator delegate, int limit) {
        this.delegate = delegate;
        this.limit = limit;
    }

    @Override
    public void moveToStart() {
        rowCount = 0;
        currentRow = OFF_ROW;
        delegate.moveToStart();
    }

    @Override
    public boolean moveNext() {
        if (rowCount >= limit) {
            currentRow = OFF_ROW;
            return false;
        }
        if (delegate.moveNext()) {
            rowCount++;
            currentRow = delegate.currentRow();
            return true;
        }
        currentRow = OFF_ROW;
        return false;
    }

    @Override
    public Row currentRow() {
        return currentRow;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (allLoaded()) {
            return CompletableFutures.failedFuture(new IllegalStateException("Iterator already fully loaded"));
        }
        return delegate.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return rowCount >= limit || delegate.allLoaded();
    }
}
