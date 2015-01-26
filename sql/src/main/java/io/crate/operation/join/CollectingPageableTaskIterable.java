/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.join;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.EmptyRewindableIterator;
import io.crate.core.collections.RewindableIterator;
import io.crate.executor.Page;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.executor.SinglePageTaskResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;


/**
 * iterable over a PageableTask
 * that gathers the pages it fetches.
 *
 * The iterator it returns iterates over all the pages
 */
public class CollectingPageableTaskIterable extends RelationIterable {

    private AtomicReference<PageableTaskResult> currentTaskResult;
    private List<Page> pages;

    public CollectingPageableTaskIterable(PageableTaskResult taskResult, PageInfo pageInfo) {
        super(pageInfo);
        this.currentTaskResult = new AtomicReference<>(taskResult);
        this.pages = new LinkedList<>();
        this.pages.add(
                taskResult.page()
        );
    }

    @Override
    public ListenableFuture<Long> fetchPage(PageInfo pageInfo) throws NoSuchElementException {
        this.pageInfo(pageInfo);

        final SettableFuture<Long> future = SettableFuture.create();
        Futures.addCallback(currentTaskResult.get().fetch(pageInfo), new FutureCallback<PageableTaskResult>() {
            @Override
            public void onSuccess(@Nullable PageableTaskResult result) {
                if (result == null) {
                    future.setException(new IllegalArgumentException("PageableTaskResult is null"));
                } else {
                    currentTaskResult.set(result);
                    pages.add(result.page());
                    future.set(result.page().size());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                future.setException(t);
            }
        });
        return future;
    }

    @Override
    public boolean isComplete() {
        // last page length is 0
        return pages.get(pages.size()-1).size() < currentPageInfo().size()      // we fetched less than requested
                || currentTaskResult.get() instanceof SinglePageTaskResult;           // we only have this single page, nothing more
    }

    @Override
    public RewindableIterator<Object[]> forCurrentPage() {
        return (RewindableIterator<Object[]>)currentTaskResult.get().page().iterator();
    }

    @Override
    public RewindableIterator<Object[]> rewindableIterator() {
        List<RewindableIterator<Object[]>> transformed = Lists.transform(pages, new Function<Page, RewindableIterator<Object[]>>() {
            @Nullable
            @Override
            public RewindableIterator<Object[]> apply(@Nullable Page input) {
                Preconditions.checkNotNull(input);
                return (RewindableIterator<Object[]>)input.iterator();
            }
        });
        return new MultiRewindableIterator<>(transformed);
    }

    @Override
    public void close() throws IOException {
        currentTaskResult.get().close();
    }

    private static class MultiRewindableIterator<T> implements RewindableIterator<T> {

        private final List<RewindableIterator<T>> iters;
        private int pos;
        private RewindableIterator<T> currentIterator;

        private MultiRewindableIterator(List<RewindableIterator<T>> iterList) {
            this.iters = iterList;
            this.pos = 0;
            if (iters.isEmpty()) {
                this.currentIterator = EmptyRewindableIterator.empty();
            } else {
                this.currentIterator = iters.get(pos);
            }
        }


        @Override
        public int rewind(int positions) {
            int left = positions - currentIterator.rewind(positions);
            while (left > 0 && pos > 0) {
                pos--;
                currentIterator = iters.get(pos);
                left = left - currentIterator.rewind(left);
            }
            return positions - left;
        }

        private boolean switchIterators() {
            pos++;
            if (pos >= iters.size()) {
                return false;
            }

            currentIterator = iters.get(pos);
            // advance through 0 length iters
            while (!currentIterator.hasNext()) {
                pos++;
                if (pos >= iters.size()) {
                    return false;
                }
                currentIterator = iters.get(pos);
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            return currentIterator.hasNext() || switchIterators();
        }

        @Override
        public T next() {
            return currentIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }
}
