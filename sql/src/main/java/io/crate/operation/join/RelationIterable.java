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

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.pageable.PageInfo;
import io.crate.executor.pageable.PageableTaskResult;
import io.crate.executor.TaskResult;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

abstract class RelationIterable implements Iterable<Object[]>, Closeable {

    private PageInfo pageInfo;

    protected RelationIterable(PageInfo pageInfo) {
        this.pageInfo = pageInfo;
    }

    public abstract ListenableFuture<Void> fetchPage(PageInfo pageInfo) throws NoSuchElementException;

    public abstract boolean isComplete();

    public PageInfo currentPageInfo() {
        return pageInfo;
    }

    protected void pageInfo(PageInfo pageInfo) {
        this.pageInfo = pageInfo;
    }

    public Iterator<Object[]> forCurrentPage() {
        Iterator<Object[]> iter = iterator();
        Iterators.advance(iter, currentPageInfo().position());
        return iter;
    }

    static RelationIterable forTaskResult(TaskResult taskResult, PageInfo pageInfo, boolean collecting) {
        if (taskResult instanceof PageableTaskResult) {
            if (collecting) {
                return new CollectingPageableTaskIterable((PageableTaskResult) taskResult, pageInfo);
            } else {
                return new SinglePagePageableTaskIterable((PageableTaskResult)taskResult, pageInfo);
            }
        } else {
            return new FetchedRowsIterable(taskResult, pageInfo);
        }
    }
}
