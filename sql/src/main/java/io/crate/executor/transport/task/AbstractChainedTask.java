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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.TaskExecutionException;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public abstract class AbstractChainedTask<T extends TaskResult> implements Task<T> {

    protected List<ListenableFuture<TaskResult>> upstreamResult = ImmutableList.of();
    protected final List<ListenableFuture<T>> resultList;
    protected final SettableFuture<T> result;

    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected AbstractChainedTask() {
        this.result = SettableFuture.create();
        this.resultList = ImmutableList.<ListenableFuture<T>>of(this.result);
    }


    @Override
    public void start() {
        if (!this.upstreamResult.isEmpty()) {
            Futures.addCallback(Futures.allAsList(this.upstreamResult), new FutureCallback<List<TaskResult>>() {
                @Override
                @SuppressWarnings("unchecked")
                public void onSuccess(@Nullable List<TaskResult> result) {
                    doStart(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    throw new TaskExecutionException(AbstractChainedTask.this, t);
                }
            });
        } else {
            doStart(ImmutableList.<TaskResult>of());
        }
    }

    /**
     * execute the actual work this task has to do.
     *
     * This method is started when the upstream task has finished
     * because we might need its results.
     *
     * @param upstreamResults the results of the upstream task
     */
    protected abstract void doStart(List<TaskResult> upstreamResults);

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        this.upstreamResult = result;
    }

    @Override
    public List<ListenableFuture<T>> result() {
        return resultList;
    }

    protected void warnNotAcknowledged(String operationName) {
        logger.warn("{} was not acknowledged. This could lead to inconsistent state.",
                operationName);
    }
}
