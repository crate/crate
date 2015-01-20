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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.RewindableIterator;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.executor.SinglePageTaskResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An iterable that only keeps the current page.
 */
public class SinglePagePageableTaskIterable extends RelationIterable {

    private AtomicReference<PageableTaskResult> currentTaskResult;

    protected SinglePagePageableTaskIterable(PageableTaskResult taskResult, PageInfo pageInfo) {
        super(pageInfo);
        this.currentTaskResult = new AtomicReference<>(taskResult);
    }

    @Override
    public ListenableFuture<Long> fetchPage(PageInfo pageInfo) {
        final SettableFuture<Long> future = SettableFuture.create();
        pageInfo(pageInfo);

        Futures.addCallback(currentTaskResult.get().fetch(pageInfo), new FutureCallback<PageableTaskResult>() {
            @Override
            public void onSuccess(@Nullable PageableTaskResult result) {
                if (result == null) {
                    future.setException(new IllegalArgumentException("PageableTaskResult is null"));
                } else {
                    currentTaskResult.set(result);
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
        PageableTaskResult currentResult = currentTaskResult.get();
        return currentResult.page().size() < currentPageInfo().size()
                || currentResult instanceof SinglePageTaskResult;
    }

    @Override
    public RewindableIterator<Object[]> rewindableIterator() {
        // all Page instances return RewindableIterators
        return (RewindableIterator<Object[]>)currentTaskResult.get().page().iterator();
    }

    @Override
    public RewindableIterator<Object[]> forCurrentPage() {
        return rewindableIterator();
    }

    @Override
    public void close() throws IOException {
        currentTaskResult.get().close();
    }

}
