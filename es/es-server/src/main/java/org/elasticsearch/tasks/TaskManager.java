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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager extends AbstractComponent {

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);

    private final ConcurrentMap<Long, Task> tasks = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final AtomicLong taskIdGenerator = new AtomicLong();


    public TaskManager(Settings settings) {
        super(settings);
    }

    /**
     * Registers a task without parent task
     * <p>
     * Returns the task manager tracked task or null if the task doesn't support the task manager
     */
    public Task register(String type, String action, TaskAwareRequest request) {
        Task task = request.createTask(taskIdGenerator.incrementAndGet(), request.getParentTask());
        if (task == null) {
            return null;
        }
        assert task.getParentTaskId().equals(request.getParentTask()) : "Request [ " + request + "] didn't preserve it parentTaskId";
        if (logger.isTraceEnabled()) {
            logger.trace("register {} [{}] [{}] [{}]", task.getId(), type, action, task.getDescription());
        }

        Task previousTask = tasks.put(task.getId(), task);
        assert previousTask == null;
        return task;
    }

    /**
     * Unregister the task
     */
    public Task unregister(Task task) {
        logger.trace("unregister task for id: {}", task.getId());
        return tasks.remove(task.getId());
    }

    /**
     * Returns the list of currently running tasks on the node
     */
    public Map<Long, Task> getTasks() {
        return Collections.unmodifiableMap(new HashMap<>(this.tasks));
    }

    /**
     * Returns a task with given id, or null if the task is not found.
     */
    public Task getTask(long id) {
        return tasks.get(id);
    }

    /**
     * Blocks the calling thread, waiting for the task to vanish from the TaskManager.
     */
    public void waitForTaskCompletion(Task task, long untilInNanos) {
        while (System.nanoTime() - untilInNanos < 0) {
            if (getTask(task.getId()) == null) {
                return;
            }
            try {
                Thread.sleep(WAIT_FOR_COMPLETION_POLL.millis());
            } catch (InterruptedException e) {
                throw new ElasticsearchException("Interrupted waiting for completion of [{}]", e, task);
            }
        }
        throw new ElasticsearchTimeoutException("Timed out waiting for completion of [{}]", task);
    }
}
