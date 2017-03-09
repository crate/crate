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

import java.util.concurrent.CompletableFuture;

/**
 * Component that expects {@link #batchSize()} * {@link #onItem(Object)} calls before {@link #processBatch(boolean)}
 * is called to compute a result based on the previously received items.
 *
 * This is for bulk/batch style processing where {@link #processBatch(boolean)} involves (network) I/O.
 *
 * @param <I> type of the items
 * @param <R> type of the result
 */
public interface BatchAccumulator<I, R> {

    /**
     * Used to feed a item into the BatchAccumulator.
     * Should be called {@link #batchSize()} times before {@link #processBatch(boolean)} is called
     */
    void onItem(I item);

    /**
     * @return number of {@link #onItem(Object)} calls that should be made before calling {@link #processBatch(boolean)}
     */
    int batchSize();

    /**
     * Process the previously received items. This is likely a operation that involves network I/O
     *
     * @param isLastBatch Indicates that this is the last processBatch call, as there won't be any more items to process.
     *                    If this is true, any resources can be released eagerly to avoid extra network-round-trips
     *                    on the following {@link #close()} call.
     */
    CompletableFuture<R> processBatch(boolean isLastBatch);

    /**
     * Closes the BatchAccumulator, releasing any resources that were used.
     */
    void close();

    /**
     * Discard any items or state previously received or created via {@link #onItem(Object)} and {@link #processBatch(boolean)}.
     *
     * BatchAccumulator implementations which have side-effects (state changes, writes to disk) should raise an
     * Exception instead of implementing it.
     *
     * @throws UnsupportedOperationException raised if previous {@link #onItem(Object)} or {@link #processBatch(boolean)}
     *                                       calls had side-effects which cannot be discarded and if repeating the same
     *                                       operation wouldn't be safe.
     */
    void reset();
}
