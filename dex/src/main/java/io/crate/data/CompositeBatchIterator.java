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

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation that is backed by multiple other BatchIterators.
 * <p>
 * It will consume each iterator to it's end before consuming the next iterator in order to make sure that repeated
 * consumption via {@link #moveToStart()} always returns the same result.
 */
public class CompositeBatchIterator<T> implements BatchIterator<T> {

    private final BatchIterator<T>[] iterators;
    private int idx = 0;

    /**
     * @param iterators underlying iterators to use; order of consumption may change if some of them are unloaded
     *                  to prefer loaded iterators over unloaded.
     */
    @SafeVarargs
    public CompositeBatchIterator(BatchIterator<T>... iterators) {
        assert iterators.length > 0 : "Must have at least 1 iterator";

        // prefer loaded iterators over unloaded to improve performance in case only a subset of data is consumed
        Comparator<BatchIterator<T>> comparing = Comparator.comparing(BatchIterator::allLoaded);
        Arrays.sort(iterators, comparing.reversed());
        this.iterators = iterators;
    }

    @Override
    public T currentElement() {
        return iterators[idx].currentElement();
    }

    @Override
    public void moveToStart() {
        for (BatchIterator iterator : iterators) {
            iterator.moveToStart();
        }
        idx = 0;
    }

    @Override
    public boolean moveNext() {
        while (idx < iterators.length) {
            BatchIterator iterator = iterators[idx];
            if (iterator.moveNext()) {
                return true;
            }
            if (iterator.allLoaded() == false) {
                return false;
            }
            idx++;
        }
        idx = 0;
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
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded()) {
                continue;
            }
            return iterator.loadNextBatch();
        }
        return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already fully loaded"));
    }

    @Override
    public boolean allLoaded() {
        for (BatchIterator iterator : iterators) {
            if (iterator.allLoaded() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        for (BatchIterator iterator : iterators) {
            iterator.kill(throwable);
        }
    }
}
