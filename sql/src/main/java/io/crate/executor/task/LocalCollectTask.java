/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.operation.QueryResultRowDownstream;
import io.crate.operation.collect.CollectOperation;
import io.crate.operation.collect.JobCollectContext;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.util.Collections;
import java.util.List;
import java.util.UUID;


/**
 * A collect task which returns one future and runs a collectOperation locally
 */
public class LocalCollectTask extends JobTask {

    private final List<ListenableFuture<TaskResult>> resultList;
    private final JobCollectContext jobCollectContext;

    public LocalCollectTask(UUID jobId,
                            CollectOperation collectOperation,
                            CollectNode collectNode,
                            CircuitBreaker circuitBreaker) {
        super(jobId);
        SettableFuture<TaskResult> result = SettableFuture.create();
        resultList = Collections.singletonList((ListenableFuture<TaskResult>) result);
        result.addListener(new Runnable() {
            @Override
            public void run() {
                jobCollectContext.close();
            }
        }, MoreExecutors.directExecutor());
        jobCollectContext = new JobCollectContext(
                collectOperation,
                jobId,
                collectNode,
                new QueryResultRowDownstream(result),
                RamAccountingContext.forExecutionNode(circuitBreaker, collectNode));
    }

    @Override
    public void start() {
        jobCollectContext.start();
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // ignored, comes always first
    }
}
