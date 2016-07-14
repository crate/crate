/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.jobs.ESJobContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.TransportAction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class EsJobContextTask extends JobTask {

    protected final List<SettableFuture<TaskResult>> results;
    protected final int executionPhaseId;
    private final JobContextService jobContextService;
    private JobExecutionContext.Builder builder;

    EsJobContextTask(UUID jobId,
                     int executionPhaseId,
                     int numResults,
                     JobContextService jobContextService) {
        super(jobId);
        this.executionPhaseId = executionPhaseId;
        this.jobContextService = jobContextService;
        results = new ArrayList<>(numResults);
    }

    void createContextBuilder(String operationName,
                              List<? extends ActionRequest> requests,
                              List<? extends ActionListener> listeners,
                              TransportAction transportAction,
                              @Nullable FlatProjectorChain projectorChain) {
        ESJobContext esJobContext = new ESJobContext(executionPhaseId, operationName,
            requests, listeners, results, transportAction, projectorChain);
        builder = jobContextService.newBuilder(jobId());
        builder.addSubContext(esJobContext);
    }

    @Override
    public void execute(RowReceiver rowReceiver) {
        assert builder != null : "Context must be created first";
        SettableFuture<TaskResult> result = results.get(0);
        try {
            JobExecutionContext ctx = jobContextService.createContext(builder);
            ctx.start();
        } catch (Throwable throwable) {
            result.setException(throwable);
        }
        JobTask.resultToRowReceiver(result, rowReceiver);
    }

    @Override
    public final List<? extends ListenableFuture<TaskResult>> executeBulk() {
        assert builder != null : "Builder must be created first";
        try {
            jobContextService.createContext(builder).start();
        } catch (Throwable throwable) {
            for (SettableFuture<TaskResult> result : results) {
                result.setException(throwable);
            }
        }
        return results;
    }
}
