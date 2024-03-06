/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.jobs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.Logger;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.concurrent.CompletionListenable;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.TaskMissing;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.profile.ProfilingContext;
import io.crate.profile.Timer;

public class RootTask implements CompletionListenable<Void> {

    private final UUID jobId;
    private final AtomicInteger numActiveTasks;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Logger logger;
    private final String coordinatorNodeId;
    private final JobsLogs jobsLogs;
    private final CompletableFuture<Void> finishedFuture = new CompletableFuture<>();
    private final AtomicBoolean killTasksOngoing = new AtomicBoolean(false);
    private final Collection<String> participatedNodes;
    private final String user;
    private final List<Task> orderedTasks;

    @Nullable
    private final ProfilingContext profiler;
    private final boolean traceEnabled;
    private volatile Throwable failure;

    @Nullable
    private final ConcurrentHashMap<Integer, Timer> taskTimersByPhaseId;
    @Nullable
    private final CompletableFuture<Map<String, Object>> profilingFuture;

    public static class Builder {

        private final Logger logger;
        private final UUID jobId;
        private final String coordinatorNode;
        private final JobsLogs jobsLogs;
        private final List<Task> tasks = new ArrayList<>();
        private final String user;
        private final Collection<String> participatingNodes;

        @Nullable
        private ProfilingContext profilingContext = null;

        Builder(Logger logger,
                UUID jobId,
                String user,
                String coordinatorNode,
                Collection<String> participatingNodes,
                JobsLogs jobsLogs) {
            this.logger = logger;
            this.jobId = jobId;
            this.user = user;
            this.coordinatorNode = coordinatorNode;
            this.participatingNodes = participatingNodes;
            this.jobsLogs = jobsLogs;
        }

        public Builder profilingContext(ProfilingContext profilingContext) {
            this.profilingContext = profilingContext;
            return this;
        }

        public void addTask(Task task) {
            assert tasks.stream().noneMatch(x -> x.id() == task.id()) : "Task with id=" + task.id() + " already registered. " + tasks;
            tasks.add(task);
        }

        boolean isEmpty() {
            return tasks.isEmpty();
        }

        int size() {
            return tasks.size();
        }

        public UUID jobId() {
            return jobId;
        }

        RootTask build() throws Exception {
            return new RootTask(
                logger,
                jobId,
                user,
                coordinatorNode,
                participatingNodes,
                jobsLogs,
                tasks,
                profilingContext
            );
        }
    }

    private RootTask(Logger logger,
                     UUID jobId,
                     String user,
                     String coordinatorNodeId,
                     Collection<String> participatingNodes,
                     JobsLogs jobsLogs,
                     List<Task> orderedTasks,
                     @Nullable ProfilingContext profilingContext) throws Exception {
        this.logger = logger;
        this.user = user;
        this.coordinatorNodeId = coordinatorNodeId;
        this.participatedNodes = participatingNodes;
        this.jobId = jobId;
        this.jobsLogs = jobsLogs;
        this.orderedTasks = orderedTasks;
        int numTasks = orderedTasks.size();
        this.numActiveTasks = new AtomicInteger(numTasks);
        this.traceEnabled = logger.isTraceEnabled();
        if (profilingContext == null) {
            taskTimersByPhaseId = null;
            profilingFuture = null;
            profiler = null;
        } else {
            profiler = profilingContext;
            taskTimersByPhaseId = new ConcurrentHashMap<>(numTasks);
            profilingFuture = new CompletableFuture<>();
        }
        for (Task task : orderedTasks) {
            jobsLogs.operationStarted(task.id(), jobId, task.name(), task::bytesUsed);
            task.completionFuture().whenComplete(new TaskFinishedListener(task.id()));
        }
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

    @Nullable
    public CompletableFuture<Void> start() {
        if (closed.get()) {
            logger.trace("job={} killed before start was called", jobId);
            return CompletableFuture.completedFuture(null); // got killed before start was called
        }
        return start(0);
    }

    private CompletableFuture<Void> start(int taskIndex) {
        for (int i = taskIndex; i < orderedTasks.size(); i++) {
            var task = orderedTasks.get(i);
            int phaseId = task.id();
            if (profiler != null) {
                String subContextName = ProfilingContext.generateProfilingKey(phaseId, task.name());
                if (taskTimersByPhaseId.put(phaseId, profiler.createAndStartTimer(subContextName)) != null) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException("Timer for " + phaseId + " already added"));
                }
            }
            try {
                logger.trace("Starting task job={} phase={} name={}", jobId, phaseId, task.name());
                CompletableFuture<Void> started = task.start();
                if (started != null) {
                    return started.thenCompose(ignored -> start(taskIndex + 1));
                }
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        }
        var localFailure = failure; // 1 volatile read
        if (localFailure != null) {
            return CompletableFuture.failedFuture(localFailure);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <T extends Task> T getTaskOrNull(int phaseId) {
        for (Task task : orderedTasks) {
            if (task.id() == phaseId) {
                return (T) task;
            }
        }
        return null;
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
    public long kill(@Nullable String reason) {
        int numKilled = 0;
        if (!closed.getAndSet(true)) {
            logger.trace("kill called on Task {}", jobId);
            if (numActiveTasks.get() == 0) {
                finish();
            } else {
                for (Task task : orderedTasks) {
                    if (task.completionFuture().isDone()) {
                        continue;
                    }
                    if (traceEnabled) {
                        logger.trace("Task kill id={} ctx={}", task.id(), task);
                    }
                    task.kill(JobKilledException.of(reason));
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
            if (logger.isTraceEnabled()) {
                logger.trace("Profiling is enabled. Task will not be closed until results are collected!");
                logger.trace("Profiling results for job {}: {}", jobId, profiler.getDurationInMSByTimer());
            }
            assert profilingFuture != null : "profilingFuture must not be null";
            try {
                Map<String, Object> executionTimes = executionTimes();
                profilingFuture.complete(executionTimes);
            } catch (Throwable t) {
                profilingFuture.completeExceptionally(t);
            }
            if (failure != null) {
                finishedFuture.completeExceptionally(failure);
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
            return CompletableFuture.failedFuture(stateException);
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

    public String userName() {
        return user;
    }

    @Override
    public String toString() {
        return "Task{" +
               "id=" + jobId +
               ", tasks=" + orderedTasks +
               ", closed=" + closed +
               '}';
    }

    private final class TaskFinishedListener implements BiConsumer<Void, Throwable> {

        private final int id;

        private TaskFinishedListener(int id) {
            this.id = id;
        }

        /**
         * Finish {@link RootTask} if this was the last active task
         * @return true if removed task was the last task, otherwise false
         */
        private boolean finishIfNeeded() {
            if (traceEnabled) {
                Task task = getTask(id);
                logger.trace("Task completed id={} task={} error={}", id, task, failure);
            }
            if (numActiveTasks.decrementAndGet() == 0) {
                finish();
                return true;
            }
            return false;
        }

        private void onSuccess() {
            jobsLogs.operationFinished(id, jobId, null);
            finishIfNeeded();
        }

        private void onFailure(@NotNull Throwable t) {
            t = SQLExceptions.unwrap(t);
            failure = t;
            jobsLogs.operationFinished(id, jobId, SQLExceptions.messageOf(t));
            if (finishIfNeeded()) {
                return;
            }
            if (killTasksOngoing.compareAndSet(false, true)) {
                for (Task task : orderedTasks) {
                    if (task.id() == id || task.completionFuture().isDone()) {
                        continue;
                    }
                    if (traceEnabled) {
                        logger.trace("Task id={} failed, killing other task={}", id, task);
                    }
                    task.kill(t);
                }
            }
        }

        @Override
        public void accept(Void result, Throwable throwable) {
            if (profiler != null) {
                stopTaskTimer();
            }
            if (throwable == null) {
                onSuccess();
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
