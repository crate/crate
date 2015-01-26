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
import io.crate.core.bigarray.IterableBigArray;

/**
 * A PageableTaskResult implementation that has all its rows at its fingertips.
 * Paging is only iterating through the backingArray, if that is exceeded, paging
 * reached the end.
 */
public class FetchedRowsPageableTaskResult extends AbstractBigArrayPageableTaskResult {

    private final long currentPosition;

    public FetchedRowsPageableTaskResult(IterableBigArray<Object[]> backingArray,
                                         long backingArrayStartIndex,
                                         PageInfo pageInfo) {
        super(backingArray, backingArrayStartIndex, pageInfo);
        this.currentPosition = pageInfo.position();
    }

    /**
     * Simply get a new TaskResult with the same backingArray with the new pageInfo on it.
     */
    @Override
    public ListenableFuture<PageableTaskResult> fetch(PageInfo pageInfo){
        PageableTaskResult result;
        long pageInfoPositionIncrement = pageInfo.position() - currentPosition;
        if (backingArrayStartIdx + pageInfoPositionIncrement >= backingArray.size()) {
            result = PageableTaskResult.EMPTY_PAGEABLE_RESULT;
        } else {
            result = new FetchedRowsPageableTaskResult(backingArray, backingArrayStartIdx + pageInfoPositionIncrement, pageInfo);
        }
        return Futures.immediateFuture(result);
    }
}
