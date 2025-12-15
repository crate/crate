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

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import org.jspecify.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;

/**
 * BatchIterator implementation that is backed by {@link Iterable}
 */
public class InMemoryBatchIterator<T> implements BatchIterator<T> {

    private final Iterable<? extends T> items;
    private final T sentinel;
    private final boolean hasLazyResultSet;

    private volatile Throwable killed = null;
    private Iterator<? extends T> it;
    private T current;

    public static <T> BatchIterator<T> empty(@Nullable T sentinel) {
        return of(Collections.emptyList(), sentinel, false);
    }

    public static <T> BatchIterator<T> of(T item, @Nullable T sentinel) {
        return of(Collections.singletonList(item), sentinel, false);
    }

    /**
     * @param items An iterable over the items. It has to be repeatable if {@code moveToStart()} is used.
     * @param sentinel the value for {@link #currentElement()} if un-positioned
     * @param hasLazyResultSet See {@link BatchIterator#hasLazyResultSet()}
     */
    public static <T> BatchIterator<T> of(Iterable<? extends T> items, @Nullable T sentinel, boolean hasLazyResultSet) {
        return new InMemoryBatchIterator<>(items, sentinel, hasLazyResultSet);
    }

    public InMemoryBatchIterator(Iterable<? extends T> items, T sentinel, boolean hasLazyResultSet) {
        this.items = items;
        this.it = items.iterator();
        this.current = sentinel;
        this.sentinel = sentinel;
        this.hasLazyResultSet = hasLazyResultSet;
    }

    @Override
    public T currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        it = items.iterator();
        current = sentinel;
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        if (it.hasNext()) {
            current = it.next();
            return true;
        }
        current = sentinel;
        return false;
    }

    @Override
    public void close() {
        killed = BatchIterator.CLOSED;
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        throw new IllegalStateException("All batches already loaded");
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public void kill(Throwable throwable) {
        killed = throwable;
    }

    @Override
    public boolean hasLazyResultSet() {
        return hasLazyResultSet;
    }

    private void raiseIfKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
    }
}
