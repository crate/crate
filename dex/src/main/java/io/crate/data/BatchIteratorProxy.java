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
import java.util.function.Supplier;

/**
 * BatchIterator implementation that acts as a proxy for another BatchIterator which isn't immediately available.
 *
 * {@link #loadTrigger} will be used on the first {@link #loadNextBatch()} the request the BatchIterator to which
 * it will delegate to once it's available.
 */
public class BatchIteratorProxy implements BatchIterator {

    private final Supplier<? extends CompletionStage<? extends BatchIterator>> loadTrigger;
    private final ColumnsProxy columnsProxy;

    private BatchIterator delegate = null;
    private CompletionStage<? extends BatchIterator> loadingFuture = null;

    public static BatchIterator newInstance(Supplier<CompletableFuture<BatchIterator>> loadTrigger, int numColumns) {
        return new CloseAssertingBatchIterator(new BatchIteratorProxy(loadTrigger, numColumns));
    }

    private BatchIteratorProxy(Supplier<? extends CompletionStage<BatchIterator>> loadTrigger,
                               int numColumns) {
        this.loadTrigger = loadTrigger;
        this.columnsProxy = new ColumnsProxy(numColumns);
    }

    @Override
    public Columns rowData() {
        return columnsProxy;
    }

    @Override
    public void moveToStart() {
        if (delegate != null) {
            delegate.moveToStart();
        }
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        if (delegate == null) {
            return false;
        }
        return delegate.moveNext();
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (delegate == null) {
            if (loadingFuture == null) {
                loadingFuture = loadTrigger.get().whenComplete((bi, failure) -> {
                    delegate = bi;
                    columnsProxy.setDelegate(bi.rowData());
                });
                return loadingFuture;
            }
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loading"));
        }
        return delegate.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        if (delegate == null) {
            return false;
        }
        return delegate.allLoaded();
    }

    private void raiseIfLoading() {
        if (loadingFuture != null && delegate == null) {
            throw new IllegalStateException("BatchIterator is loading");
        }
    }


    private class ColumnsProxy implements Columns {

        private final int numColumns;
        private final ProxyInput[] inputs;

        ColumnsProxy(int numColumns) {
            this.numColumns = numColumns;
            inputs = new ProxyInput[numColumns];
            for (int i = 0; i < numColumns; i++) {
                inputs[i] = new ProxyInput();
            }
        }

        @Override
        public Input<?> get(int index) {
            return inputs[index];
        }

        @Override
        public int size() {
            return numColumns;
        }

        public void setDelegate(Columns columns) {
            for (int i = 0; i < inputs.length; i++) {
                inputs[i].input = columns.get(i);
            }
        }
    }

    private static class ProxyInput implements Input {

        Input<?> input;

        @Override
        public Object value() {
            return input.value();
        }
    }
}
