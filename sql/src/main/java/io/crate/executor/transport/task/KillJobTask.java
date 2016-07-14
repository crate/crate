/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.operation.projectors.RowReceiver;

import java.util.List;
import java.util.UUID;

public class KillJobTask extends JobTask {

    private final UUID jobToKill;
    private final TransportKillJobsNodeAction nodeAction;

    public KillJobTask(TransportKillJobsNodeAction nodeAction,
                       UUID jobId,
                       UUID jobToKill) {
        super(jobId);
        this.nodeAction = nodeAction;
        this.jobToKill = jobToKill;
    }

    @Override
    public void execute(RowReceiver rowReceiver) {
        KillJobsRequest request = new KillJobsRequest(ImmutableList.of(jobToKill));
        nodeAction.executeKillOnAllNodes(request,
            new OneRowActionListener<>(rowReceiver, KillTask.KILL_RESPONSE_TO_ROW_FUNCTION));
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> executeBulk() {
        throw new UnsupportedOperationException("kill job task cannot be executed as bulk operation");
    }
}
