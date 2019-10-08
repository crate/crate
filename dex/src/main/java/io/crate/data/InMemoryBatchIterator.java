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

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * BatchIterator implementation that is backed by {@link Iterable}
 */
public class InMemoryBatchIterator<T> implements BatchIterator<T> {

    private final Iterable<? extends T> items;
    private final T sentinel;
    private final boolean involvesIO;

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
     * @param involvesIO true if consuming the items or properties of the items can involve disk or network IO
     */
    public static <T> BatchIterator<T> of(Iterable<? extends T> items, @Nullable T sentinel, boolean involvesIO) {
        return new CloseAssertingBatchIterator<>(new InMemoryBatchIterator<>(items, sentinel, involvesIO));
    }

    @VisibleForTesting
    public InMemoryBatchIterator(Iterable<? extends T> items, T sentinel, boolean involvesIO) {
        this.items = items;
        this.it = items.iterator();
        this.current = sentinel;
        this.sentinel = sentinel;
        this.involvesIO = involvesIO;
    }

    @Override
    public T currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        it = items.iterator();
        current = sentinel;
    }

    @Override
    public boolean moveNext() {
        if (it.hasNext()) {
            current = it.next();
            return true;
        }
        current = sentinel;
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return CompletableFuture.failedFuture(new IllegalStateException("All batches already loaded"));
    }

    @Override
    public boolean allLoaded() {
        return true;
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        // handled by CloseAssertingBatchIterator
    }

    @Override
    public boolean involvesIO() {
        return involvesIO;
    }
}
