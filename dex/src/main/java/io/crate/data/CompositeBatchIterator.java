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

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation that is backed by multiple other BatchIterators.
 *
 * It will consume each iterator to it's end before consuming the next iterator in order to make sure that repeated
 * consumption via {@link #moveToStart()} always returns the same result.
 */
public class CompositeBatchIterator implements BatchIterator {

    private final BatchIterator[] iterators;
    private final CompositeColumns columns;
    private int idx = 0;

    private boolean loading = false;

    /**
     * @param iterators underlying iterators to use; order of consumption may change if some of them are unloaded
     *                  to prefer loaded iterators over unloaded.
     */
    public CompositeBatchIterator(BatchIterator ... iterators) {
        assert iterators.length > 0 : "Must have at least 1 iterator";

        // prefer loaded iterators over unloaded to improve performance in case only a subset of data is consumed
        Arrays.sort(iterators, Comparator.comparing(BatchIterator::allLoaded).reversed());
        this.iterators = iterators;
        this.columns = new CompositeColumns();
    }

    @Override
    public Columns rowData() {
        return columns;
    }

    @Override
    public void moveToStart() {
        raiseIfLoading();
        for (BatchIterator iterator : iterators) {
            iterator.moveToStart();
        }
        resetIndex();
    }

    private void resetIndex() {
        idx = 0;
        columns.updateInputs();
    }

    @Override
    public boolean moveNext() {
        raiseIfLoading();
        while (idx < iterators.length) {
            BatchIterator iterator = iterators[idx];
            if (iterator.moveNext()) {
                return true;
            }
            if (iterator.allLoaded() == false) {
                return false;
            }
            idx++;
            columns.updateInputs();
        }
        resetIndex();
        return false;
    }

    @Override
    public void close() {
        for (BatchIterator iterator : iterators) {
            iterator.close();
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (loading) {
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loading"));
        }
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded()) {
                continue;
            }
            loading = true;
            return iterator.loadNextBatch().whenComplete((r, t) -> loading = false);
        }
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already fully loaded"));
    }

    @Override
    public boolean allLoaded() {
        raiseIfLoading();
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded() == false) {
                return false;
            }
        }
        return true;
    }

    private void raiseIfLoading() {
        if (loading) {
            throw new IllegalStateException("BatchIterator already loading");
        }
    }

    private class CompositeColumns implements Columns {

        private final int numColumns;

        private ProxyInput[] inputs;

        CompositeColumns() {
            numColumns = iterators[0].rowData().size();
            inputs = new ProxyInput[numColumns];
            for (int i = 0; i < inputs.length; i++) {
                inputs[i] = new ProxyInput();
            }
            updateInputs();
        }

        void updateInputs() {
            if (idx >= iterators.length) {
                return;
            }
            Columns columns = iterators[idx].rowData();
            for (int i = 0; i < numColumns; i++) {
                inputs[i].input = columns.get(i);
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
    }

}
