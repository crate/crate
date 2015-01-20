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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

class ImmediateTestTask extends JobTask {

    private final List<ListenableFuture<TaskResult>> result;

    public ImmediateTestTask(Object[][] rows, int limit, int offset) {
        super(UUID.randomUUID());
        Object[][] limitedRows = Arrays.copyOfRange(rows,
                Math.min(offset, rows.length),
                limit < 0 ? rows.length : Math.min(limit, rows.length)
        );
        this.result = ImmutableList.of(
                Futures.<TaskResult>immediateFuture(new QueryResult(limitedRows)));
    }


    @Override
    public void start() {
        // ignore
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return result;
    }

    @Override
    public void upstreamResult(List result) {
        // ignore
    }
}
