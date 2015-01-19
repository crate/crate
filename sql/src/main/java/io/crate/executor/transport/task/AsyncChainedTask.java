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

package io.crate.executor.transport.task;

import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.RowCountResult;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Provides basic upstreamResult/result handling for tasks that return the number of affected rows in their result.
 * This class will chain the results and create the sum of their results or set it to -1 if any of the results returned -1
 *
 * Implementations have to set the result on {@link #result} and just have to implement {@link #start()}.
 */
public abstract class AsyncChainedTask extends JobTask {

    protected final SettableFuture<TaskResult> result;
    private final List<ListenableFuture<TaskResult>> resultList;

    protected AsyncChainedTask(UUID jobId) {
        super(jobId);
        result = SettableFuture.create();
        ListenableFuture<TaskResult> resultFallback = Futures.withFallback(result, new FutureFallback<TaskResult>() {
            @Override
            public ListenableFuture<TaskResult> create(@Nonnull Throwable t) throws Exception {
                return Futures.<TaskResult>immediateFuture(RowCountResult.error(t));
            }
        });
        resultList = new ArrayList<>();
        resultList.add(resultFallback);
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        resultList.addAll(result);
    }
}
