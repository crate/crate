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
import io.crate.planner.node.PlanNode;

import java.util.Collection;
import java.util.List;

/**
 * I can create and execute taskzzz
 */
public interface TaskExecutor {

    /**
     * create new tasks from a plan node in the context of a job
     * identified by <code>jobId</code>.
     *
     * @param planNode the node to convert to tasks
     * @param job the job these tasks get executed in
     * @return a list of tasks ready for execution
     */
    public List<Task> newTasks(PlanNode planNode, Job job);

    /**
     * Execute a given collection of tasks.
     * Optionally given an upstream task whose result is fed to the first
     * task in <code>tasks</code>
     *
     * @param tasks a collection of tasks to be executed
     * @return a list of futures, from the last task
     */
    public List<ListenableFuture<TaskResult>> execute(Collection<Task> tasks);
}
