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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import org.elasticsearch.action.ActionListener;

import java.util.List;
import java.util.UUID;

public class KillJobTask extends JobTask {

    private final SettableFuture<TaskResult> result;
    private final List<ListenableFuture<TaskResult>> results;
    private UUID jobToKill;
    private TransportKillJobsNodeAction nodeAction;


    public KillJobTask(TransportKillJobsNodeAction nodeAction,
                       UUID jobId,
                       UUID jobToKill) {
        super(jobId);
        this.nodeAction = nodeAction;
        result = SettableFuture.create();
        results = ImmutableList.of((ListenableFuture<TaskResult>) result);
        this.jobToKill = jobToKill;
    }

    @Override
    public void start() {
        KillJobsRequest request = new KillJobsRequest(ImmutableList.of(jobToKill));

        nodeAction.executeKillOnAllNodes(request, new ActionListener<KillResponse>() {
            @Override
            public void onResponse(KillResponse killResponse) {
                result.set(new RowCountResult(killResponse.numKilled()));
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
    }
}
