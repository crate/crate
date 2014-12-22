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

package io.crate.executor.task.join;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.*;
import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.common.breaker.CircuitBreaker;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class NestedLoopTask extends JobTask {

    private NestedLoopOperation operation;
    private final SettableFuture<TaskResult> result = SettableFuture.create();
    private final List<ListenableFuture<TaskResult>> results = Arrays.<ListenableFuture<TaskResult>>asList(result);

    public NestedLoopTask(UUID jobId,
                          String nodeId,
                          NestedLoopNode nestedLoopNode,
                          TaskExecutor executor,
                          ProjectionToProjectorVisitor projectionToProjectorVisitor,
                          CircuitBreaker circuitBreaker) {
        super(jobId);
        String ramContextId = String.format(Locale.ENGLISH, "%s: %s", nodeId, jobId.toString());
        RamAccountingContext ramAccountingContext = new RamAccountingContext(
                ramContextId,
                circuitBreaker);
        operation = new NestedLoopOperation(nestedLoopNode, executor, projectionToProjectorVisitor, ramAccountingContext, jobId);
    }

    @Override
    public void start() {
        ListenableFuture<Object[][]> operationFuture = operation.execute();
        Futures.addCallback(operationFuture, new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                result.set(new QueryResult(rows));
            }

            @Override
            public void onFailure(Throwable t) {
                result.setException(t);
            }
        });
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // ignore
    }

}
