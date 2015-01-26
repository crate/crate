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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.bigarray.IterableBigArray;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import io.crate.executor.*;

import java.util.List;
import java.util.UUID;

class PageableTestTask extends JobTask implements PageableTask {

    private IterableBigArray<Object[]> backingArray;
    private SettableFuture<TaskResult> result;

    protected PageableTestTask(Object[][] rows, int limit, int offset) {
        super(UUID.randomUUID());
        backingArray = new MultiNativeArrayBigArray<Object[]>(offset, limit, rows);
        result = SettableFuture.create();
    }

    @Override
    public void start(PageInfo pageInfo) {
        result.set(new FetchedRowsPageableTaskResult(backingArray, 0, pageInfo));
    }

    @Override
    public void start() {
        // ignore
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return ImmutableList.<ListenableFuture<TaskResult>>of(result);
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // ignore
    }
}
