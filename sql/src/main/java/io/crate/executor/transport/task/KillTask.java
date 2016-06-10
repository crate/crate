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

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.FutureActionListener;
import io.crate.executor.JobTask;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillAllRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class KillTask extends JobTask {

    static final Function<KillResponse, TaskResult> RESPONSE_TO_TASK_RESULT = new Function<KillResponse, TaskResult>() {
        @Nullable
        @Override
        public TaskResult apply(@Nullable KillResponse input) {
            return input == null ? null : new RowCountResult(input.numKilled());
        }
    };
    private final TransportKillAllNodeAction nodeAction;

    public KillTask(TransportKillAllNodeAction nodeAction, UUID jobId) {
        super(jobId);
        this.nodeAction = nodeAction;
    }

    @Override
    public ListenableFuture<TaskResult> execute() {
        FutureActionListener<KillResponse, TaskResult> listener = new FutureActionListener<>(RESPONSE_TO_TASK_RESULT);
        nodeAction.broadcast(new KillAllRequest(), listener);
        return listener;
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> executeBulk() {
        throw new UnsupportedOperationException("kill task cannot be executed as bulk operation");
    }
}
