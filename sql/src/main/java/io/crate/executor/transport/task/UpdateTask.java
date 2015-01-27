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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.RowCountResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportExecutor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class UpdateTask extends AsyncChainedTask {

    private final TransportExecutor transportExecutor;
    private final List<Task> subTasks;

    public UpdateTask(TransportExecutor transportExecutor, UUID jobId, List<Task> subTasks) {
        super(jobId);
        this.transportExecutor = transportExecutor;
        this.subTasks = subTasks;
    }

    @Override
    public void start() {
        if (subTasks.size() == 0) {
            result.set(RowCountResult.EMPTY_RESULT);
            return;
        }
        final List<ListenableFuture<TaskResult>> subTasksResult = transportExecutor.execute(subTasks);
        Futures.addCallback(Futures.allAsList(subTasksResult), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> results) {
                if (results == null) {
                    result.setException(new NullPointerException());
                    return;
                }
                //assert results.size() == 1 : "Last sub-task is expected to have 1 result only";
                try {
                    result.set(new RowCountResult(((Number) results.get(0).rows()[0][0]).longValue()));
                } catch (Throwable t) {
                    result.setException(t);
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                if (t == null) {
                    t = new NullPointerException();
                }
                result.setException(t);
            }
        });
    }

}
