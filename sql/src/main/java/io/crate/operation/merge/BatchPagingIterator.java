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

package io.crate.operation.merge;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * BatchIterator implementation that is backed by a {@link PagingIterator}.
 *
 * It needs an upstream with which it communicates via
 *
 *  - {@link #tryFetchMore},        (to request more data from an upstream)
 *  - {@link #isUpstreamExhausted}, (to check if an upstream can deliver more data)
 *  - {@link #closeCallback}        (called once the iterator is closed)
 *
 *  - {@link #completeLoad(Throwable)}  (used by the upstream to inform the
 *                                       BatchPagingIterator that the pagingIterator has been filled)
 */
public class BatchPagingIterator<Key> implements BatchIterator<Row> {

    private final PagingIterator<Key, Row> pagingIterator;
    private final Function<Key, Boolean> tryFetchMore;
    private final BooleanSupplier isUpstreamExhausted;
    private final Runnable closeCallback;

    private Iterator<Row> it;
    private CompletableFuture<Void> currentlyLoading;

    private boolean closed = false;
    private volatile Throwable killed;
    private Row current;

    public BatchPagingIterator(PagingIterator<Key, Row> pagingIterator,
                               Function<Key, Boolean> tryFetchMore,
                               BooleanSupplier isUpstreamExhausted,
                               Runnable closeCallback) {
        this.pagingIterator = pagingIterator;
        this.it = pagingIterator;
        this.tryFetchMore = tryFetchMore;
        this.isUpstreamExhausted = isUpstreamExhausted;
        this.closeCallback = closeCallback;
    }

    @Override
    public Row currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        raiseIfClosedOrKilled();
        this.it = pagingIterator.repeat().iterator();
        current = null;
    }

    @Override
    public boolean moveNext() {
        raiseIfClosedOrKilled();

        if (it.hasNext()) {
            current = it.next();
            return true;
        }
        current = null;
        return false;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            closeCallback.run();
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        String illegalState = getIllegalState();
        if (illegalState == null) {
            currentlyLoading = new CompletableFuture<>();
            if (tryFetchMore.apply(pagingIterator.exhaustedIterable())) {
                return currentlyLoading;
            }
            return CompletableFutures.failedFuture(new IllegalStateException("Although isLoaded is false, tryFetchMoreFailed"));
        }
        return CompletableFutures.failedFuture(new IllegalStateException(illegalState));
    }

    @Nullable
    private String getIllegalState() {
        if (closed) {
            return "Iterator is closed";
        }
        if (allLoaded()) {
            return "All data already loaded";
        }
        return null;
    }

    @Override
    public boolean allLoaded() {
        return isUpstreamExhausted.getAsBoolean();
    }

    public void completeLoad(@Nullable Throwable t) {
        if (currentlyLoading == null) {
            if (t == null) {
                killed = new IllegalStateException("completeLoad called without having called loadNextBatch");
            } else {
                killed = t;
            }
            return;
        }
        if (t == null) {
            currentlyLoading.complete(null);
        } else {
            currentlyLoading.completeExceptionally(t);
        }
    }

    private void raiseIfClosedOrKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
    }
}
