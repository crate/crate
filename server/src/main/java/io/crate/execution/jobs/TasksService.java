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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.jetbrains.annotations.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.concurrent.CountdownFuture;
import io.crate.exceptions.TaskMissing;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.kill.KillAllListener;
import io.crate.user.Role;

@Singleton
public class TasksService extends AbstractLifecycleComponent {

    private static final Logger LOGGER = LogManager.getLogger(TasksService.class);

    private final ClusterService clusterService;
    private final JobsLogs jobsLogs;
    private final ConcurrentMap<UUID, RootTask> activeTasks =
        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final List<KillAllListener> killAllListeners = new CopyOnWriteArrayList<>();

    private final Object failedSentinel = new Object();
    private final Cache<UUID, Object> recentlyFailed = Caffeine.newBuilder()
        .executor(Runnable::run)
        .maximumSize(200L)
        .expireAfterWrite(30, TimeUnit.SECONDS)
        .build();

    @Inject
    public TasksService(ClusterService clusterService, JobsLogs jobsLogs) {
        this.clusterService = clusterService;
        this.jobsLogs = jobsLogs;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        for (RootTask rootTask : activeTasks.values()) {
            rootTask.kill("TasksService stopped");
        }
    }

    public void addListener(KillAllListener listener) {
        killAllListeners.add(listener);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    public RootTask getTask(UUID jobId) {
        RootTask rootTask = activeTasks.get(jobId);
        if (rootTask == null) {
            throw new TaskMissing(TaskMissing.Type.ROOT, jobId);
        }
        return rootTask;
    }

    public Stream<UUID> getJobIdsByCoordinatorNode(final String coordinatorNodeId) {
        return activeTasks.values()
            .stream()
            .filter(task -> task.coordinatorNodeId().equals(coordinatorNodeId))
            .map(RootTask::jobId);
    }

    public Stream<UUID> getJobIdsByParticipatingNodes(final String nodeId) {
        return activeTasks.values().stream()
            .filter(i -> i.participatingNodes().contains(nodeId))
            .map(RootTask::jobId);
    }

    @Nullable
    public RootTask getTaskOrNull(UUID jobId) {
        return activeTasks.get(jobId);
    }

    @VisibleForTesting
    public RootTask.Builder newBuilder(UUID jobId) {
        return new RootTask.Builder(
            LOGGER,
            jobId,
            Role.CRATE_USER.name(),
            clusterService.localNode().getId(),
            Collections.emptySet(),
            jobsLogs);
    }

    public RootTask.Builder newBuilder(UUID jobId,
                                       String user,
                                       String coordinatorNodeId,
                                       Collection<String> participatingNodes) {
        return new RootTask.Builder(LOGGER, jobId, user, coordinatorNodeId, participatingNodes, jobsLogs);
    }

    public int numActive() {
        return activeTasks.size();
    }

    public RootTask createTask(RootTask.Builder builder) throws Exception {
        if (builder.isEmpty()) {
            throw new IllegalArgumentException("RootTask.Builder must at least contain 1 Task");
        }
        final UUID jobId = builder.jobId();
        RootTask newRootTask = builder.build();

        TaskCallback taskCallback = new TaskCallback(jobId);
        newRootTask.completionFuture().whenComplete(taskCallback);

        RootTask existing = activeTasks.putIfAbsent(jobId, newRootTask);
        if (existing != null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "task for job %s already exists:%n%s", jobId, existing));
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                "RootTask created for job={} tasks={} totalTasks={}",
                jobId,
                builder.size(),
                activeTasks.size()
            );
        }
        return newRootTask;
    }


    /**
     * kills all tasks which the user is allowed to kill and are active at the time of the call of this method.
     * The super-user can kill any tasks, other users can only kill their own tasks.
     *
     * @return a future holding the number of tasks kill was called on, the future is finished when all tasks
     * are completed and never fails.
     */
    public CompletableFuture<Integer> killAll(String userName) {
        boolean isSuperUser = userName.equals(Role.CRATE_USER.name());
        if (isSuperUser) {
            for (KillAllListener killAllListener : killAllListeners) {
                try {
                    killAllListener.killAllJobs();
                } catch (Throwable t) {
                    LOGGER.error("Failed to call killAllJobs on listener={} error={}", killAllListener, t);
                }
            }
        }
        Collection<UUID> toKill = List.copyOf(activeTasks.keySet());
        if (toKill.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        return killTasks(toKill, userName, null);
    }

    private CompletableFuture<Integer> killTasks(Collection<UUID> toKill, String userName, @Nullable String reason) {
        assert !toKill.isEmpty() : "toKill must not be empty";
        int numKilled = 0;
        CountdownFuture countDownFuture = new CountdownFuture(toKill.size());
        boolean isSuperUser = userName.equals(Role.CRATE_USER.name());
        for (UUID jobId : toKill) {
            RootTask ctx = activeTasks.get(jobId);
            if (ctx == null) {
                // no kill but we need to count down
                countDownFuture.onSuccess();
                continue;
            }
            // superuser can always kill jobs; normal users only their own jobs
            if (isSuperUser || ctx.userName().equals(userName)) {
                recentlyFailed.put(jobId, failedSentinel);
                ctx.completionFuture().whenComplete(countDownFuture);
                ctx.kill(reason);
                numKilled++;
            } else {
                // no kill but we need to count down
                countDownFuture.onSuccess();
            }
        }
        final int finalNumKilled = numKilled;
        return countDownFuture.handle((r, f) -> finalNumKilled);
    }

    public CompletableFuture<Integer> killJobs(Collection<UUID> toKill, String userName, @Nullable String reason) {
        boolean isSuperUser = userName.equals(Role.CRATE_USER.name());
        if (isSuperUser) {
            for (KillAllListener killAllListener : killAllListeners) {
                for (UUID job : toKill) {
                    try {
                        killAllListener.killJob(job);
                    } catch (Throwable t) {
                        LOGGER.error("Failed to call killJob on listener={}, err={}", killAllListener, t);
                    }
                }
            }
        }
        return killTasks(toKill, userName, reason);
    }

    /**
     * @return true if the job has been recently removed due to a failure (e.g. has been killed).
     *         false if it is UNKNOWN if it failed.
     *
     *         This may return false negatives, but never false positives.
     */
    public boolean recentlyFailed(UUID jobId) {
        return recentlyFailed.getIfPresent(jobId) == failedSentinel;
    }

    private class TaskCallback implements BiConsumer<Void, Throwable> {

        private final UUID jobId;

        TaskCallback(UUID jobId) {
            this.jobId = jobId;
        }

        @Override
        public void accept(Void aVoid, Throwable throwable) {
            activeTasks.remove(jobId);
            if (throwable != null) {
                recentlyFailed.put(jobId, failedSentinel);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("RootTask removed from active tasks: jobId={} remainingTasks={} failure={}",
                             jobId, activeTasks.size(), throwable);
            }
        }
    }

    @VisibleForTesting
    public void logActiveTasksToError() {
        if (LOGGER.isErrorEnabled()) {
            String localNodeId = clusterService.localNode().getId();
            for (var entry : activeTasks.entrySet()) {
                LOGGER.error("Active task node={} jobId={}, task={}", localNodeId, entry.getKey(), entry.getValue());
            }
        }
    }
}
