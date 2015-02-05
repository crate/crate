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
import io.crate.executor.pageable.Page;
import io.crate.executor.pageable.PageInfo;
import io.crate.executor.pageable.PageableTaskResult;
import io.crate.executor.pageable.SinglePageTaskResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public class SinglePagePageableTaskIterable extends RelationIterable {

    private final PageableTaskResult taskResult;
    private volatile Page currentPage;

    protected SinglePagePageableTaskIterable(PageableTaskResult taskResult, PageInfo pageInfo) {
        super(pageInfo);
        this.taskResult = taskResult;
        this.currentPage = taskResult.page();
    }

    @Override
    public ListenableFuture<Void> fetchPage(PageInfo pageInfo) throws NoSuchElementException {
        this.pageInfo(pageInfo);

        final SettableFuture<Void> future = SettableFuture.create();
        Futures.addCallback(taskResult.fetch(pageInfo), new FutureCallback<Page>() {
            @Override
            public void onSuccess(@Nullable Page result) {
                if (result == null) {
                    future.setException(new IllegalArgumentException("PageableTaskResult is null"));
                }
                currentPage = result;
                future.set(null);
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
        return currentPage.size() < currentPageInfo().size()
                || taskResult instanceof SinglePageTaskResult;
    }

    @Override
    public Iterator<Object[]> forCurrentPage() {
        return iterator();
    }

    @Override
    public void close() throws IOException {
        taskResult.close();
    }

    @Override
    public Iterator<Object[]> iterator() {
        return currentPage.iterator();
    }
}
