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

package io.crate.executor;


import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.concurrent.ForwardingFutureCallback;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

public class PageableTaskResult<T extends Closeable> implements TaskResult {


    private final PageableTask<T> task;
    private final PageInfo pageInfo;
    private final Page page;
    private T context;

    public PageableTaskResult(PageableTask<T> task, PageInfo pageInfo, Page page, T context) {
        this.task = task;
        this.pageInfo = pageInfo;
        this.page = page;
        this.context = context;
    }

    @Override
    public Object[][] rows() {
        return Iterables.toArray(page, Object[].class);
    }

    @Override
    public ListenableFuture<TaskResult> fetch(final PageInfo newPageInfo) {
        if (context == null) {
            throw new IllegalStateException("TaskResult already closed");
        }

        if (newPageInfo.position() == pageInfo.position()) {
            if (newPageInfo.size() == pageInfo.size()) {
                return Futures.immediateFuture((TaskResult) this);
            } else if (newPageInfo.size() > pageInfo.size()) {
                return pageWithOverlap(newPageInfo);
            } else {
                throw new UnsupportedOperationException("fetching a smaller page than the current one is not supported");
            }
        }

        if (newPageInfo.position() == (pageInfo.position() + pageInfo.size())) {
            final SettableFuture<TaskResult> future = SettableFuture.create();
            FutureCallback<TaskResult> forwardingFutureCallback = new ForwardingFutureCallback<>(future);
            task.fetchMore(newPageInfo, context, forwardingFutureCallback);
            return future;
        }

        // gap or backwards paging
        return fetchNew(newPageInfo);
    }

    private ListenableFuture<TaskResult> pageWithOverlap(final PageInfo newPageInfo) {
        final SettableFuture<TaskResult> future = SettableFuture.create();
        PageInfo remainingPageInfo = new PageInfo(pageInfo.position() + pageInfo.size(), newPageInfo.size() - pageInfo.size());

        task.fetchMore(remainingPageInfo, context, new FutureCallback<TaskResult>() {
            @Override
            public void onSuccess(TaskResult result) {
                future.set(new PageableTaskResult<>(task, newPageInfo, Pages.concat(page, result.page()), context));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                future.setException(t);
            }
        });
        return future;
    }

    private ListenableFuture<TaskResult> fetchNew(PageInfo newPageInfo) {
        final SettableFuture<TaskResult> future = SettableFuture.create();
        FutureCallback<TaskResult> forwardingFutureCallback = new ForwardingFutureCallback<>(future);
        task.fetchNew(newPageInfo, context, forwardingFutureCallback);
        context = null;
        return future;
    }

    @Override
    public Page page() {
        return page;
    }

    @Nullable
    @Override
    public String errorMessage() {
        return null;
    }

    @Override
    public void close() throws IOException {
        if (context != null) {
            context.close();
            context = null;
        }
    }
}
