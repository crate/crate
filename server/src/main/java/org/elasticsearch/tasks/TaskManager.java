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

import static io.crate.common.unit.TimeValue.timeValueMillis;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import io.crate.common.unit.TimeValue;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager implements ClusterStateApplier {

    private static final Logger LOGGER = LogManager.getLogger(TaskManager.class);

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);

    private final ConcurrentMap<Long, Task> tasks = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final AtomicLong taskIdGenerator = new AtomicLong();

    private final Map<TaskId, String> banedParents = new ConcurrentHashMap<>();

    private DiscoveryNodes lastDiscoveryNodes = DiscoveryNodes.EMPTY_NODES;

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

    /**
     * Returns the list of currently running tasks on the node
     */
    public Map<Long, Task> getTasks() {
        return Collections.unmodifiableMap(this.tasks);
    }


    /**
     * Returns a task with given id, or null if the task is not found.
     */
    @Nullable
    public Task getTask(long id) {
        Task task = tasks.get(id);
        return task;
    }

    /**
     * Returns the number of currently banned tasks.
     * <p>
     * Will be used in task manager stats and for debugging.
     */
    public int getBanCount() {
        return banedParents.size();
    }

    /**
     * Bans all tasks with the specified parent task from execution, cancels all tasks that are currently executing.
     * <p>
     * This method is called when a parent task that has children is cancelled.
     */
    public void setBan(TaskId parentTaskId, String reason) {
        LOGGER.trace("setting ban for the parent task {} {}", parentTaskId, reason);

        // Set the ban first, so the newly created tasks cannot be registered
        synchronized (banedParents) {
            if (lastDiscoveryNodes.nodeExists(parentTaskId.getNodeId())) {
                // Only set the ban if the node is the part of the cluster
                banedParents.put(parentTaskId, reason);
            }
        }
    }

    /**
     * Removes the ban for the specified parent task.
     * <p>
     * This method is called when a previously banned task finally cancelled
     */
    public void removeBan(TaskId parentTaskId) {
        LOGGER.trace("removing ban for the parent task {}", parentTaskId);
        banedParents.remove(parentTaskId);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        lastDiscoveryNodes = event.state().getNodes();
        if (event.nodesRemoved()) {
            synchronized (banedParents) {
                lastDiscoveryNodes = event.state().getNodes();
                // Remove all bans that were registered by nodes that are no longer in the cluster state
                Iterator<TaskId> banIterator = banedParents.keySet().iterator();
                while (banIterator.hasNext()) {
                    TaskId taskId = banIterator.next();
                    if (lastDiscoveryNodes.nodeExists(taskId.getNodeId()) == false) {
                        LOGGER.debug("Removing ban for the parent [{}] on the node [{}], reason: the parent node is gone", taskId,
                            event.state().getNodes().getLocalNode());
                        banIterator.remove();
                    }
                }
            }
        }
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
