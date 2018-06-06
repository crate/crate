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

package io.crate.execution.jobs;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.annotations.VisibleForTesting;
import io.crate.concurrent.CompletableFutures;
import io.crate.concurrent.CompletionListenable;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.TaskMissing;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class RootTask implements CompletionListenable<Void> {

    private static final Logger LOGGER = Loggers.getLogger(RootTask.class);

    private final UUID jobId;
    private final ConcurrentMap<Integer, Task> tasksByPhaseId;
    private final AtomicInteger numTasks;
    private final IntArrayList orderedTaskIds;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String coordinatorNodeId;
    private final JobsLogs jobsLogs;
    private final CompletableFuture<Void> finishedFuture = new CompletableFuture<>();
    private final AtomicBoolean killTasksOngoing = new AtomicBoolean(false);
    private final Collection<String> participatedNodes;

    @Nullable
    private final ProfilingContext profiler;
    private volatile Throwable failure;

    @Nullable
    private final ConcurrentHashMap<Integer, Timer> taskTimersByPhaseId;
    @Nullable
    private final CompletableFuture<Map<String, Object>> profilingFuture;

    public static class Builder {

        private final UUID jobId;
        private final String coordinatorNode;
        private final JobsLogs jobsLogs;
        private final List<Task> tasks = new ArrayList<>();
        private final Collection<String> participatingNodes;

        @Nullable
        private ProfilingContext profilingContext = null;

        Builder(UUID jobId, String coordinatorNode, Collection<String> participatingNodes, JobsLogs jobsLogs) {
            this.jobId = jobId;
            this.coordinatorNode = coordinatorNode;
            this.participatingNodes = participatingNodes;
            this.jobsLogs = jobsLogs;
        }

        public Builder profilingContext(ProfilingContext profilingContext) {
            this.profilingContext = profilingContext;
            return this;
        }

        public void addTask(Task task) {
            tasks.add(task);
        }

        boolean isEmpty() {
            return tasks.isEmpty();
        }

        public UUID jobId() {
            return jobId;
        }

        RootTask build() throws Exception {
            return new RootTask(
                jobId, coordinatorNode, participatingNodes, jobsLogs, tasks, profilingContext);
        }
    }


    private RootTask(UUID jobId,
                     String coordinatorNodeId,
                     Collection<String> participatingNodes,
                     JobsLogs jobsLogs,
                     List<Task> orderedTasks,
                     @Nullable ProfilingContext profilingContext) throws Exception {
        this.coordinatorNodeId = coordinatorNodeId;
        this.participatedNodes = participatingNodes;
        this.jobId = jobId;
        this.jobsLogs = jobsLogs;

        int numTasks = orderedTasks.size();

        if (profilingContext == null) {
            taskTimersByPhaseId = null;
            profilingFuture = null;
            profiler = null;
        } else {
            profiler = profilingContext;
            taskTimersByPhaseId = new ConcurrentHashMap<>(numTasks);
            profilingFuture = new CompletableFuture<>();
        }

        orderedTaskIds = new IntArrayList(numTasks);
        tasksByPhaseId = new ConcurrentHashMap<>(numTasks);
        this.numTasks = new AtomicInteger(numTasks);

        boolean traceEnabled = LOGGER.isTraceEnabled();
        for (Task task : orderedTasks) {
            int phaseId = task.id();
            orderedTaskIds.add(phaseId);

            task.completionFuture().whenComplete(new RemoveTaskListener(phaseId));

            if (tasksByPhaseId.put(phaseId, task) != null) {
                throw new IllegalArgumentException("Task for " + phaseId + " already added");
            }
            if (profiler != null) {
                String subContextName = String.format(Locale.ROOT, "%d-%s", task.id(), task.name());
                if (taskTimersByPhaseId.put(phaseId, profiler.createTimer(subContextName)) != null) {
                    throw new IllegalArgumentException("Timer for " + phaseId + " already added");
                }
            }
            if (traceEnabled) {
                LOGGER.trace("adding subContext {}, now there are {} tasksByPhaseId", phaseId, tasksByPhaseId.size());
            }
        }
        prepare(orderedTasks);
    }

    public UUID jobId() {
        return jobId;
    }

    String coordinatorNodeId() {
        return coordinatorNodeId;
    }

    Collection<String> participatingNodes() {
        return participatedNodes;
    }

    private void prepare(List<Task> orderedTasks) throws Exception {
        for (int i = 0; i < orderedTaskIds.size(); i++) {
            int id = orderedTaskIds.get(i);
            Task task = orderedTasks.get(i);
            jobsLogs.operationStarted(id, jobId, task.name());
            try {
                task.prepare();
            } catch (Exception e) {
                for (; i >= 0; i--) {
                    id = orderedTaskIds.get(i);
                    task = orderedTasks.get(i);
                    task.cleanup();
                    jobsLogs.operationFinished(id, jobId, "Prepare: " + SQLExceptions.messageOf(e), -1);
                }
                throw e;
            }
        }
    }

    public void start() throws Throwable {
        for (IntCursor id : orderedTaskIds) {
            Task task = tasksByPhaseId.get(id.value);
            if (task == null || closed.get()) {
                break; // got killed before start was called
            }
            if (profiler != null) {
                assert taskTimersByPhaseId != null : "taskTimersByPhaseId must not be null";
                taskTimersByPhaseId.get(id.value).start();
            }
            task.start();
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Nullable
    public <T extends Task> T getTaskOrNull(int phaseId) {
        //noinspection unchecked
        return (T) tasksByPhaseId.get(phaseId);
    }

    public <T extends Task> T getTask(int phaseId) throws TaskMissing {
        T task = getTaskOrNull(phaseId);
        if (task == null) {
            throw new TaskMissing(TaskMissing.Type.CHILD, jobId, phaseId);
        }
        return task;
    }

    /**
     * Issues a kill on all active tasks. This method returns immediately. The caller should use the future
     * returned by {@link CompletionListenable#completionFuture()} to track EOL of the task.
     *
     * @return the number of tasks on which kill was called
     */
    public long kill() {
        int numKilled = 0;
        if (!closed.getAndSet(true)) {
            LOGGER.trace("kill called on Task {}", jobId);
            if (numTasks.get() == 0) {
                finish();
            } else {
                for (Task task : tasksByPhaseId.values()) {
                    // kill will trigger the ContextCallback onClose too
                    // so it is not necessary to remove the task from the map here as it will be done in the callback
                    task.kill(null);
                    numKilled++;
                }
            }
        }
        return numKilled;
    }

    private void close() {
        if (failure != null) {
            finishedFuture.completeExceptionally(failure);
        } else {
            finishedFuture.complete(null);
        }
    }

    private void finish() {
        if (profiler != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Profiling is enabled. Task will not be closed until results are collected!");
                LOGGER.trace("Profiling results for job {}: {}", jobId, profiler.getDurationInMSByTimer());
            }
            assert profilingFuture != null : "profilingFuture must not be null";
            try {
                Map<String, Object> executionTimes = executionTimes();
                profilingFuture.complete(executionTimes);
            } catch (Throwable t) {
                profilingFuture.completeExceptionally(t);
            }
        } else {
            close();
        }
    }

    public CompletableFuture<Map<String, Object>> finishProfiling() {
        if (profiler == null) {
            // sanity check
            IllegalStateException stateException = new IllegalStateException(
                String.format(Locale.ENGLISH, "Tried to finish profiling job [id=%s], but profiling is not enabled.", jobId));
            return CompletableFutures.failedFuture(stateException);
        }
        assert profilingFuture != null : "profilingFuture must not be null";
        return profilingFuture.whenComplete((o, t) -> close());
    }

    @VisibleForTesting
    Map<String, Object> executionTimes() {
        if (profiler == null) {
            return Collections.emptyMap();
        }
        return profiler.getDurationInMSByTimer();
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return finishedFuture;
    }

    @Override
    public String toString() {
        return "Task{" +
               "id=" + jobId +
               ", tasksByPhaseId=" + tasksByPhaseId.values() +
               ", closed=" + closed +
               '}';
    }

    private final class RemoveTaskListener implements BiConsumer<CompletionState, Throwable> {

        private final int id;

        private RemoveTaskListener(int id) {
            this.id = id;
        }

        /**
         * Remove task and finish {@link RootTask}
         * @return true if removed task was the last task, otherwise false
         */
        private boolean removeAndFinishIfNeeded() {
            Task removed = tasksByPhaseId.remove(id);
            assert removed != null : "removed must not be null";
            if (numTasks.decrementAndGet() == 0) {
                finish();
                return true;
            }
            return false;
        }

        public void onSuccess(@Nullable CompletionState state) {
            assert state != null : "state must not be null";
            jobsLogs.operationFinished(id, jobId, null, state.bytesUsed());
            removeAndFinishIfNeeded();
        }

        public void onFailure(@Nonnull Throwable t) {
            failure = t;
            jobsLogs.operationFinished(id, jobId, SQLExceptions.messageOf(t), -1);
            if (removeAndFinishIfNeeded()) {
                return;
            }
            if (killTasksOngoing.compareAndSet(false, true)) {
                LOGGER.trace("onFailure killing all other tasksByPhaseId..");
                for (Task subContext : tasksByPhaseId.values()) {
                    subContext.kill(t);
                }
            }
        }

        @Override
        public void accept(CompletionState completionState, Throwable throwable) {
            if (profiler != null) {
                stopTaskTimer();
            }
            if (throwable == null) {
                onSuccess(completionState);
            } else {
                onFailure(throwable);
            }
        }

        private void stopTaskTimer() {
            assert profiler != null : "profiler must not be null";
            assert taskTimersByPhaseId != null : "taskTimersByPhaseId must not be null";
            Timer removed = taskTimersByPhaseId.remove(id);
            assert removed != null : "removed must not be null";
            profiler.stopTimerAndStoreDuration(removed);
        }
    }
}
