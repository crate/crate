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

package org.elasticsearch.tasks;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager {

    private static final Logger LOGGER = LogManager.getLogger(TaskManager.class);

    private final ConcurrentMap<Long, Task> tasks = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final AtomicLong taskIdGenerator = new AtomicLong();

    public TaskManager() {
    }

    /**
     * Registers a task without parent task
     */
    public Task register(String type, String action, TaskAwareRequest request) {
        Task task = request.createTask(taskIdGenerator.incrementAndGet(), type, action, request.getParentTask());
        Objects.requireNonNull(task);
        assert task.getParentTaskId().equals(request.getParentTask()) : "Request [ " + request + "] didn't preserve it parentTaskId";
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("register {} [{}] [{}] [{}]", task.getId(), type, action, task.getDescription());
        }

        Task previousTask = tasks.put(task.getId(), task);
        assert previousTask == null;
        return task;
    }

    /**
     * Unregister the task
     */
    public Task unregister(Task task) {
        LOGGER.trace("unregister task for id: {}", task.getId());
        return tasks.remove(task.getId());
    }
}
