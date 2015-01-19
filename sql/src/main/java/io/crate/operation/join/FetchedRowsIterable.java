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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.executor.TaskResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

class FetchedRowsIterable extends RelationIterable {

    private final Iterable<Object[]> rows;

    public FetchedRowsIterable(TaskResult taskResult, PageInfo pageInfo) {
        super(pageInfo);
        if (taskResult instanceof PageableTaskResult) {
            this.rows = ((PageableTaskResult) taskResult).page();
        } else {
            this.rows = Arrays.asList(taskResult.rows());
        }
    }

    @Override
    public Iterator<Object[]> iterator() {
        return rows.iterator();
    }

    @Override
    public ListenableFuture<Void> fetchPage(PageInfo pageInfo) {
        this.pageInfo(pageInfo);
        return Futures.immediateFuture(null);
    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public void close() throws IOException {
        // ayayayayayaaaay!
    }
}
