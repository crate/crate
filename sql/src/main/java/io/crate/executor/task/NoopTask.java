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

package io.crate.executor.task;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;

import java.util.List;

public class NoopTask implements Task {

    private static final List<ListenableFuture<TaskResult>> EMPTY_RESULT =
        ImmutableList.of(
            Futures.immediateFuture((TaskResult) TaskResult.EMPTY_RESULT)
        );
    public static final NoopTask INSTANCE_EMPTY_RESULT = new NoopTask(EMPTY_RESULT);

    private static final List<ListenableFuture<TaskResult>> ZERO_ROWS =
        ImmutableList.of(
            Futures.immediateFuture((TaskResult) TaskResult.ZERO)
        );
    public static final NoopTask INSTANCE_ZERO_ROWS = new NoopTask(ZERO_ROWS);

    private final List<ListenableFuture<TaskResult>> results;

    private NoopTask(List<ListenableFuture<TaskResult>> results) {
        this.results = results;
    }

    @Override
    public void start() {
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
    }
}
