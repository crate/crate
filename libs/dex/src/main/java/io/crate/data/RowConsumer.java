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

package io.crate.data;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.jetbrains.annotations.Nullable;

import io.crate.common.concurrent.Killable;

/**
 * A consumer of a {@link BatchIterator}.
 * <p>
 *     The consumer will start consuming the rows from the {@link BatchIterator} once
 *     {@link #accept(BatchIterator, Throwable)} is called.
 * </p>
 *
 * <p>
 *     Once the consumer has consumed all data, or as much as it needs, it calls {@link BatchIterator#close()}.
 *     An exception to this is if the throwable in the {@link #accept(BatchIterator, Throwable)} call isn't null.
 *     In that case the Consumer is not required to call close, as the iterator may even be null.
 * </p>
 * <p>
 *     Multiple calls to {@link #accept(BatchIterator, Throwable)} are not allowed.
 * </p>
 *
 * <p>
 * A RowConsumer is <b>optionally</b> {@link Killable}. Note that opposed to
 * the kill handling on the {@link BatchIterator} where its implementation and
 * use is mandatory for proper kill management, implementations of the
 * RowConsumer are free to skip implementing it and users of a RowConsumer
 * don't have to invoke the kill. That said, if it allows to accelerate a kill
 * request it should be implemented.
 *
 * Also note that RowConsumer's are not responsible for killing the BatchIterator.
 * Other components are supposed to do that.
 * </p>
 */
public interface RowConsumer extends BiConsumer<BatchIterator<Row>, Throwable>, Killable {

    /**
     * Start consumption of the given {@link BatchIterator}.
     * <p>
     *     The given iterator must be in it's start position and must be usable until {@link BatchIterator#close()} is called.
     *
     *     If {@code failure} is not null the {@code iterator} cannot be used,
     *     but instead the consumer has to process the failure.
     * </p>
     *
     * @param iterator the iterator to be consumed, if a failure is present this iterator isn't usable or null.
     * @param failure the cause of the failure or null if successful
     *
     */
    @Override
    void accept(BatchIterator<Row> iterator, @Nullable Throwable failure);

    CompletableFuture<?> completionFuture();

    /**
     * @return true if the consumer wants to scroll backwards by using {@link BatchIterator#moveToStart}
     * */
    default boolean requiresScroll() {
        return false;
    }

    @Override
    default void kill(Throwable throwable) {
    }
}
