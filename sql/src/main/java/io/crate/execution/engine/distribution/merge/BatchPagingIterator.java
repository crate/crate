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

package io.crate.execution.engine.distribution.merge;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.Exceptions;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * BatchIterator implementation that is backed by a {@link PagingIterator}.
 *
 * It needs an upstream with which it communicates via
 *
 *  - {@link #fetchMore},           (to request more data from an upstream)
 *  - {@link #isUpstreamExhausted}, (to check if an upstream can deliver more data)
 *  - {@link #closeCallback}        (called once the iterator is closed,
 *                                   will receive a throwable if the BatchIterator was killed)
 */
public class BatchPagingIterator<Key> implements BatchIterator<Row> {

    private final PagingIterator<Key, Row> pagingIterator;
    private final Function<Key, CompletableFuture<? extends Iterable<? extends KeyIterable<Key, Row>>>> fetchMore;
    private final BooleanSupplier isUpstreamExhausted;
    private final Consumer<? super Throwable> closeCallback;

    private volatile Throwable killed;
    private Iterator<Row> it;
    private boolean closed = false;
    private Row current;

    public BatchPagingIterator(PagingIterator<Key, Row> pagingIterator,
                               Function<Key, CompletableFuture<? extends Iterable<? extends KeyIterable<Key, Row>>>> fetchMore,
                               BooleanSupplier isUpstreamExhausted,
                               Consumer<? super Throwable> closeCallback) {
        this.pagingIterator = pagingIterator;
        this.it = pagingIterator;
        this.fetchMore = fetchMore;
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
            pagingIterator.finish(); // release resource, specially possible ram accounted bytes
            closeCallback.accept(killed);
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("BatchIterator already closed"));
        }
        if (allLoaded()) {
            return CompletableFuture.failedFuture(new IllegalStateException("All data already loaded"));
        }
        Throwable err = killed;
        if (err != null) {
            return CompletableFuture.failedFuture(err);
        }
        return fetchMore.apply(pagingIterator.exhaustedIterable())
            .whenComplete(this::onNextPage);
    }

    private void onNextPage(Iterable<? extends KeyIterable<Key, Row>> rows, Throwable ex) {
        if (ex == null) {
            pagingIterator.merge(rows);
            if (isUpstreamExhausted.getAsBoolean()) {
                pagingIterator.finish();
            }
        } else {
            killed = ex;
            if (ex instanceof RuntimeException) {
                throw ((RuntimeException) ex);
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public boolean allLoaded() {
        return isUpstreamExhausted.getAsBoolean();
    }

    @Override
    public boolean involvesIO() {
        return true;
    }

    private void raiseIfClosedOrKilled() {
        Throwable err = killed;
        if (err != null) {
            Exceptions.rethrowUnchecked(err);
        }
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        killed = throwable;
        close();
    }
}
