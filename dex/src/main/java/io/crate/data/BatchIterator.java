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

import java.util.concurrent.CompletionStage;


/**
 * An iterator used to navigate over batched data.
 * An iterator usually contains some data already in-memory.
 * This data can be accessed via {@link #moveNext()} and {@link #currentRow()}.
 *
 * Once all in-memory data has been consumed more data can loaded with {@link #loadNextBatch()}
 * unless {@link #allLoaded()} is true in which case the iterator is exhausted.
 *
 * A BatchIterator starts either *before* the first row or in an unloaded state.
 * This means a consumer can consume a BatchIterator like this:
 *
 * <pre>
 *     while (it.moveNext()) {
 *         // do something with the row
 *     }
 *     if (it.allLoaded()) {
 *          // iterator is exhausted
 *     } else {
 *         it.loadNextBatch().whenComplete((r, t) -> {
 *             // continue consumption
 *         }
 *     }
 * </pre>
 *
 *
 * Thread-safety notes:
 *
 * Concurrent usage of a BatchIterator is not supported.
 */
public interface BatchIterator {

    Row OFF_ROW = new Row() {
        @Override
        public int numColumns() {
            throw new IllegalStateException("Iterator is not on a row");
        }

        @Override
        public Object get(int index) {
            throw new IllegalStateException("Iterator is not on a row");
        }

        @Override
        public Object[] materialize() {
            throw new IllegalStateException("Iterator is not on a row");
        }
    };

    /**
     * Moves the Iterator back to the starting position.
     *
     * A consumer can then iterate over the rows again by calling into
     * {@link #moveNext()} and {@link #loadNextBatch()} as appropriate.
     *
     * @throws IllegalStateException if the cursor is closed
     */
    void moveToStart();

    /**
     * Advances the iterator.
     *
     * @return true if the iterator moved to the next valid row.
     *         false if the iterator is no longer on a valid row.
     *         If {@link #allLoaded()} returns true the iterator is out of data. Otherwise {@link #loadNextBatch()}
     *         can be used to load the next batch - after which {@code moveNext()} can be used again.
     *
     * @throws IllegalStateException if the cursor is closed
     */
    boolean moveNext();


    /**
     * @return the current row the iterator is positioned on.
     *         Multiple calls to this method might return the same instance, even after {@link #moveNext()}.
     *         But underlying data will change.
     * @throws IllegalStateException if the cursor is closed
     */
    Row currentRow();

    /**
     * Closes the iterator and frees all resources.
     * After this method has been called all methods on the iterator will result in an error.
     *
     * There are two exceptions to that:
     *   - close: Close itself can be called multiple times, but only the first call has an effect.
     *   - loadNextBatch: This method never raises, but instead returns a failed CompletionStage.
     */
    void close();

    /**
     * Loads the next batch if there is still data available for loading.
     * This may only be called if {@link #allLoaded()} returns false.
     * If {@link #allLoaded()} returns true, calling this method will return a failed CompletionStage.
     *
     * <p>
     * NOTE: while loading takes place the iterator must not be moved.
     * An attempt to do so will result in a IllegalStateException
     *
     * @return a future which will be completed once the loading is done.
     *         Once the future completes the iterator is still in an "off-row" state, but {@link #moveNext()}
     *         can be called again if the next batch contains more data.
     */
    CompletionStage<?> loadNextBatch();

    /**
     * @return true if all underlying data is already loaded
     * @throws IllegalStateException if the cursor is closed
     */
    boolean allLoaded();
}
