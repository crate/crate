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

package io.crate.executor;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.UUID;

/**
 * A task gets executed as part of or in the context of a
 * {@linkplain io.crate.executor.Job} and returns a result asynchronously
 * as a list of futures.
 *
 * create a task, call {@linkplain #start()} and wait for the futures returned
 * by {@linkplain #result()} to return the result or raise an exception.
 *
 * If tasks are chained, it is necessary to call {@linkplain #upstreamResult(java.util.List)}
 * with the result of the former task, so the current task can itself wait for
 * the upstream result if necessary before it starts its execution.
 *
 * @see io.crate.executor.TaskExecutor
 * @see io.crate.executor.Job
 *
 */
public interface Task {

    /**
     * the id of the job this task is executed in
     */
    UUID jobId();

    /**
     * start the execution
     */
    void start();

    /**
     * Get the result of the task execution as a list of
     * {@linkplain com.google.common.util.concurrent.ListenableFuture} instances.
     * This method may be called before {@linkplain #start()} is called.
     */
    List<ListenableFuture<TaskResult>> result();

    /**
     * let this class get to know the result of the former ("upstream") task
     * @param result the result of the "upstream" task.
     */
    void upstreamResult(List<ListenableFuture<TaskResult>> result);
}
