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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Page;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.executor.SinglePageTaskResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * iterable over a PageableTask
 * that gathers the pages it fetches.
 *
 * The iterator it returns iterates over all the pages
 */
public class CollectingPageableTaskIterable extends RelationIterable {

    private volatile PageableTaskResult currentTaskResult;
    private List<Page> pages;

    public CollectingPageableTaskIterable(PageableTaskResult taskResult, PageInfo pageInfo) {
        super(pageInfo);
        this.currentTaskResult = taskResult;
        this.pages = new LinkedList<>();
        this.pages.add(
                taskResult.page()
        );
    }

    @Override
    public Iterator<Object[]> iterator() {
        return Iterators.concat(Lists.transform(pages, new Function<Page, Iterator<Object[]>>() {
            @Nullable
            @Override
            public Iterator<Object[]> apply(@Nullable Page input) {
                if (input == null) {
                    return Collections.emptyIterator();
                } else {
                    return input.iterator();
                }
            }
        }).iterator());
    }

    @Override
    public ListenableFuture<Void> fetchPage(PageInfo pageInfo) throws NoSuchElementException {
        this.pageInfo(pageInfo);

        final SettableFuture<Void> future = SettableFuture.create();
        Futures.addCallback(currentTaskResult.fetch(pageInfo), new FutureCallback<PageableTaskResult>() {
            @Override
            public void onSuccess(@Nullable PageableTaskResult result) {
                if (result == null) {
                    future.setException(new IllegalArgumentException("PageableTaskResult is null"));
                } else {
                    currentTaskResult = result;
                    pages.add(result.page());
                    future.set(null);
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
                || currentTaskResult instanceof SinglePageTaskResult;           // we only have this single page, nothing more
    }

    @Override
    public void close() throws IOException {
        currentTaskResult.close();
    }
}
