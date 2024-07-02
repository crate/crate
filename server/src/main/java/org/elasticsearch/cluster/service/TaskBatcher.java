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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.service.MasterService.TaskInputs;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import io.crate.common.unit.TimeValue;

/**
 * Batching support for {@link PrioritizedEsThreadPoolExecutor}
 * Tasks that share the same batching key are batched (see {@link BatchedTask#batchingKey})
 */
public final class TaskBatcher<T> {

    private final Logger logger;
    private final PrioritizedEsThreadPoolExecutor threadExecutor;
    // package visible for tests
    final Map<ClusterStateTaskExecutor<T>, LinkedHashSet<BatchedTask<T>>> tasksPerBatchingKey = new HashMap<>();
    private final Consumer<TaskInputs<T>> runTasks;
    private final Executor genericExecutor;

    public TaskBatcher(Logger logger,
                       Executor genericExecutor,
                       PrioritizedEsThreadPoolExecutor threadExecutor,
                       Consumer<TaskInputs<T>> runTasks) {
        this.logger = logger;
        this.genericExecutor = genericExecutor;
        this.threadExecutor = threadExecutor;
        this.runTasks = runTasks;
    }

    public void submitTasks(List<BatchedTask<T>> tasks, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchedTask<T> firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.batchingKey == firstTask.batchingKey) :
            "tasks submitted in a batch should share the same batching key: " + tasks;
        // convert to an identity map to check for dups based on task identity
        final Map<T, BatchedTask<T>> tasksIdentity = tasks.stream().collect(Collectors.toMap(
            BatchedTask::getTask,
            Function.identity(),
            (a, b) -> {
                throw new IllegalStateException("cannot add duplicate task: " + a);
            },
            IdentityHashMap::new)
        );

        synchronized (tasksPerBatchingKey) {
            LinkedHashSet<BatchedTask<T>> existingTasks = tasksPerBatchingKey.computeIfAbsent(firstTask.batchingKey,
                k -> new LinkedHashSet<>(tasks.size()));
            for (BatchedTask<T> existing : existingTasks) {
                // check that there won't be two tasks with the same identity for the same batching key
                BatchedTask<T> duplicateTask = tasksIdentity.get(existing.getTask());
                if (duplicateTask != null) {
                    throw new IllegalStateException("task [" + duplicateTask.describeTasks(
                        Collections.singletonList(existing)) + "] with source [" + duplicateTask.source + "] is already queued");
                }
            }
            existingTasks.addAll(tasks);
        }

        if (timeout != null) {
            threadExecutor.execute(firstTask, timeout, () -> onTimeoutInternal(tasks, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    private void onTimeoutInternal(List<BatchedTask<T>> tasks, TimeValue timeout) {
        final ArrayList<BatchedTask<T>> toRemove = new ArrayList<>();
        for (BatchedTask<T> task : tasks) {
            if (task.processed.getAndSet(true) == false) {
                logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                toRemove.add(task);
            }
        }
        if (toRemove.isEmpty() == false) {
            BatchedTask<T> firstTask = toRemove.get(0);
            Object batchingKey = firstTask.batchingKey;
            assert tasks.stream().allMatch(t -> t.batchingKey == batchingKey) :
                "tasks submitted in a batch should share the same batching key: " + tasks;
            synchronized (tasksPerBatchingKey) {
                LinkedHashSet<BatchedTask<T>> existingTasks = tasksPerBatchingKey.get(batchingKey);
                if (existingTasks != null) {
                    existingTasks.removeAll(toRemove);
                    if (existingTasks.isEmpty()) {
                        tasksPerBatchingKey.remove(batchingKey);
                    }
                }
            }
            onTimeout(toRemove, timeout);
        }
    }

    /**
     * Action to be implemented by the specific batching implementation.
     * All tasks have the same batching key.
     */
    protected void onTimeout(List<BatchedTask<T>> tasks, TimeValue timeout) {
        genericExecutor.execute(() ->
            tasks.forEach(task -> task.listener.onFailure(
                task.source,
                new ProcessClusterEventTimeoutException(timeout, task.source))));
    }

    void runIfNotProcessed(BatchedTask<T> updateTask) {
        // if this task is already processed, it shouldn't execute other tasks with same batching key that arrived later,
        // to give other tasks with different batching key a chance to execute.
        if (updateTask.processed.get() == false) {
            final List<BatchedTask<T>> toExecute = new ArrayList<>();
            final Map<String, List<BatchedTask<T>>> processTasksBySource = new HashMap<>();
            synchronized (tasksPerBatchingKey) {
                LinkedHashSet<BatchedTask<T>> pending = tasksPerBatchingKey.remove(updateTask.batchingKey);
                if (pending != null) {
                    for (BatchedTask<T> task : pending) {
                        if (task.processed.getAndSet(true) == false) {
                            logger.trace("will process {}", task);
                            toExecute.add(task);
                            processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task);
                        } else {
                            logger.trace("skipping {}, already processed", task);
                        }
                    }
                }
            }

            if (toExecute.isEmpty() == false) {
                final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
                    String tasks = updateTask.describeTasks(entry.getValue());
                    return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");


                runTasks.accept(new TaskInputs<>(tasksSummary, toExecute, updateTask.batchingKey));
            }
        }
    }
}
