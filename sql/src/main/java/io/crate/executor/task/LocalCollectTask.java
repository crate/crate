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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.QueryResult;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.operation.collect.CollectOperation;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.common.breaker.CircuitBreaker;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * A collect task which returns one future and runs a  collectOperation locally and synchronous
 */
public class LocalCollectTask extends JobTask {

    private final CollectNode collectNode;
    private final CollectOperation collectOperation;
    private final List<ListenableFuture<TaskResult>> resultList;
    private final SettableFuture<TaskResult> result;
    private final RamAccountingContext ramAccountingContext;

    public LocalCollectTask(UUID jobId,
                            CollectOperation<Object[][]> collectOperation, CollectNode collectNode,
                            CircuitBreaker circuitBreaker) {
        super(jobId);
        this.collectNode = collectNode;
        this.collectOperation = collectOperation;
        this.resultList = new ArrayList<>(1);
        this.result = SettableFuture.create();
        resultList.add(result);
        ramAccountingContext = new RamAccountingContext(
                String.format("%s: %s", collectNode.id(), collectNode.jobId()),
                circuitBreaker);
    }

    @Override
    public void start() {
        Futures.addCallback(collectOperation.collect(collectNode, ramAccountingContext), new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                result.set(new QueryResult(rows));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });
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
