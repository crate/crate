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

package io.crate.executor;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.planner.Plan;
import io.crate.planner.node.PlanNode;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * This is the one who creates jobs and executes them.
 * He can also create single tasks and execute them.
 */
public interface Executor {

    /**
     * create a new job from a given {@linkplain io.crate.planner.Plan}
     * @param node the plan to create a job with tasks from
     * @return an instance of {@linkplain Job} filled with adorable tasks
     */
    public Job newJob(Plan node);

    /**
     * execute a given job asynchronously, returning a list of futures
     * (the results of the tasks the job is comprised of)
     * @param job the job to execute
     * @return a list of listenable futures with the results of the last executed task
     */
    public <T extends TaskResult> List<ListenableFuture<T>> execute(Job job);

    /**
     * create a new task from a {@linkplain io.crate.planner.node.PlanNode}
     * in the context of the job identified by <code>jobId</code>.
     */
    public Task newTask(PlanNode planNode, UUID jobId);

    /**
     * Execute a task asynchronously, returning a list of futures, the result of the task.
     *
     * @param task the task to execute
     * @param upstreamTask if not null <code>task</code> will receive this tasks
     *                     result as upstream result
     * @return a list of listenable futures with the results of the executed task
     */
    public <T extends TaskResult> List<ListenableFuture<T>> execute(Task<T> task, @Nullable Task upstreamTask);
}
