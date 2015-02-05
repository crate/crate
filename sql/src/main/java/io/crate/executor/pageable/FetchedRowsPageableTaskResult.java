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

package io.crate.executor.pageable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.executor.pageable.policy.PageCachePolicy;
import org.elasticsearch.common.lease.Releasables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * A PageableTaskResult implementation that has all its rows at its fingertips.
 * Paging is only iterating through the backingArray, if that is exceeded, paging
 * reached the end.
 */
public class FetchedRowsPageableTaskResult implements PageableTaskResult {

    private final IterableBigArray<Object[]> backingArray;
    private final long backingArrayStartIdx;
    private final Page page;

    public FetchedRowsPageableTaskResult(IterableBigArray<Object[]> backingArray,
                                         long backingArrayStartIndex,
                                         PagingContext pagingCtx) {
        Preconditions.checkArgument(
                backingArrayStartIndex + pagingCtx.pageInfo().position() < backingArray.size(),
                "backingArray exceeded");
        this.backingArray = backingArray;
        this.backingArrayStartIdx = backingArrayStartIndex;
        this.page = createPage(pagingCtx.pageInfo());
    }

    private Page createPage(PageInfo pageInfo) {
        return new BigArrayPage(backingArray, this.backingArrayStartIdx+pageInfo.position(), pageInfo.size());
    }

    @Override
    public Object[][] rows() {
        throw new UnsupportedOperationException("FetchedRowsPageableTaskResult does not support rows()");
    }

    @Nullable
    @Override
    public String errorMessage() {
        return null;
    }

    /**
     * Simply get a new TaskResult with the same backingArray with the new pageInfo on it.
     */
    @Override
    public ListenableFuture<Page> fetch(PageInfo pageInfo){
        if (backingArrayStartIdx + pageInfo.position() > backingArray.size()) {
            return Futures.immediateFailedFuture(new NoSuchElementException("backingArray exceeded"));
        }
        return Futures.immediateFuture(createPage(pageInfo));
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public PageCachePolicy policy() {
        return PageCachePolicy.NO_CACHE;
    }

    @Override
    public void close() throws IOException {
        Releasables.close(backingArray);
    }
}
