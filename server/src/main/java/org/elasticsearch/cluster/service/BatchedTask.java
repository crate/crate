/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.service;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;

import io.crate.common.collections.Lists;

/**
 * Represents a runnable task that supports batching.
 */
public final class BatchedTask<T> extends PrioritizedRunnable {

    private final TaskBatcher<T> taskBatcher;

    /**
     * whether the task has been processed already
     */
    protected final AtomicBoolean processed = new AtomicBoolean();

    /**
     * the object that is used as batching key
     */
    protected final ClusterStateTaskExecutor<T> batchingKey;
    /**
     * the task object that is wrapped
     */
    protected final T task;

    protected final ClusterStateTaskListener listener;

    public BatchedTask(TaskBatcher<T> taskBatcher,
                       Priority priority,
                       String source,
                       T task,
                       ClusterStateTaskListener listener,
                       ClusterStateTaskExecutor<T> batchingKey) {
        super(priority, source);
        this.taskBatcher = taskBatcher;
        this.batchingKey = batchingKey;
        this.task = task;
        this.listener = listener;
    }

    @Override
    public void run() {
        this.taskBatcher.runIfNotProcessed(this);
    }

    @Override
    public String toString() {
        String taskDescription = describeTasks(Collections.singletonList(this));
        if (taskDescription.isEmpty()) {
            return "[" + source + "]";
        } else {
            return "[" + source + "[" + taskDescription + "]]";
        }
    }

    public String describeTasks(List<? extends BatchedTask<T>> tasks) {
        return batchingKey.describeTasks(Lists.mapLazy(tasks, BatchedTask::getTask));
    }

    public T getTask() {
        return task;
    }
}
