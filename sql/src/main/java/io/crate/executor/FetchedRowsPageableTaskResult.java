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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.ObjectArray;

import javax.annotation.Nullable;
import java.io.IOException;

public abstract class FetchedRowsPageableTaskResult<T> implements TaskResult {

    protected final T backingArray;
    protected final int backingArrayStartIndex;
    private volatile Page currentPage;

    protected FetchedRowsPageableTaskResult(T backingArray, int backingArrayStartIndex, PageInfo pageInfo) {
        this.backingArray = backingArray;
        this.backingArrayStartIndex = backingArrayStartIndex;
        setPage(pageInfo);
    }

    private void setPage(PageInfo pageInfo) {
        if (backingArrayStartIndex + pageInfo.position() >= backingArraySize()) {
            currentPage = Page.EMPTY;
        } else {
            currentPage = newPage(pageInfo);
        }
    }

    protected abstract long backingArraySize();

    protected abstract Page newPage(PageInfo pageInfo);

    @Override
    public ListenableFuture<TaskResult> fetch(PageInfo pageInfo) {
        setPage(pageInfo);
        return Futures.<TaskResult>immediateFuture(this);
    }

    @Override
    public Page page() {
        return currentPage;
    }

    @Override
    public Object[][] rows() {
        throw new UnsupportedOperationException("rows() not supported");
    }

    @Nullable
    @Override
    public String errorMessage() {
        return null;
    }

    public static TaskResult wrap(TaskResult wrapMe, PageInfo pageInfo) {
        if (wrapMe instanceof QueryResult || wrapMe instanceof RowCountResult) {
            return forArray(wrapMe.rows(), 0, pageInfo);
        } else {
            return wrapMe;
        }
    }

    public static FetchedRowsPageableTaskResult<?> forArray(Object[][] rows, int backingArrayStartIndex, PageInfo pageInfo) {
        return new FetchedObjectRowsPageableTaskResult(rows, backingArrayStartIndex, pageInfo);
    }

    public static FetchedRowsPageableTaskResult<?> forArray(ObjectArray<Object[]> rows, int backingArrayStartIndex, PageInfo pageInfo) {
        return new FetchedBigArrayRowsPageableTaskResult(rows, backingArrayStartIndex, pageInfo);
    }

    private static class FetchedObjectRowsPageableTaskResult extends FetchedRowsPageableTaskResult<Object[][]> {

        public FetchedObjectRowsPageableTaskResult(Object[][] backingArray,
                                                   int backingArrayStartIndex,
                                                   PageInfo pageInfo) {
            super(backingArray, backingArrayStartIndex, pageInfo);
        }

        @Override
        protected long backingArraySize() {
            return backingArray.length;
        }

        @Override
        protected Page newPage(PageInfo pageInfo) {
            return new ObjectArrayPage(
                    backingArray,
                    backingArrayStartIndex + pageInfo.position(),
                    pageInfo.size());
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    private static class FetchedBigArrayRowsPageableTaskResult extends FetchedRowsPageableTaskResult<ObjectArray<Object[]>> {

        protected FetchedBigArrayRowsPageableTaskResult(ObjectArray<Object[]> backingArray, int backingArrayStartIndex, PageInfo pageInfo) {
            super(backingArray, backingArrayStartIndex, pageInfo);
        }

        @Override
        protected long backingArraySize() {
            return backingArray.size();
        }

        @Override
        protected Page newPage(PageInfo pageInfo) {
            return new BigArrayPage(backingArray, backingArrayStartIndex + pageInfo.position(), pageInfo.size());
        }

        @Override
        public void close() throws IOException {
            backingArray.close();
        }
    }
}
