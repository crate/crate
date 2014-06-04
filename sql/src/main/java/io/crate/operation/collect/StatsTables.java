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

package io.crate.operation.collect;

import io.crate.core.collections.NonBlockingArrayQueue;
import io.crate.core.collections.NoopQueue;
import io.crate.executor.Job;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import jsr166e.ConcurrentHashMapV8;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stats tables that are globally available on each node and contain meta data of the cluster
 * like active jobs
 *
 * injected via guice instead of using static so that if two nodes run
 * in the same jvm the memoryTables aren't shared between the nodes.
 */
public class StatsTables {

    private final Map<UUID, JobContext> jobsTable = new ConcurrentHashMapV8<>();
    private final Map<UUID, OperationContext> operationsTable = new ConcurrentHashMapV8<>();
    private AtomicReference<BlockingQueue<JobContextLog>> jobsLog;
    private final static NoopQueue<JobContextLog> NOOP_JOBS_LOG = new NoopQueue<>();

    private final JobsLogIterableGetter jobsLogIterableGetter;
    private final JobsIterableGetter jobsIterableGetter;
    private final OperationsIterableGetter operationsIterableGetter;

    public interface IterableGetter {
        public Iterable<?> getIterable();
    }

    @Inject
    public StatsTables(Settings settings, NodeSettingsService nodeSettingsService) {
        AtomicReference<Integer> lastLogSize = new AtomicReference<>(
                settings.getAsInt(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, 0));

        int logSize = lastLogSize.get();
        if (logSize == 0) {
            jobsLog = new AtomicReference<>((BlockingQueue<JobContextLog>)NOOP_JOBS_LOG);
        } else {
            jobsLog = new AtomicReference<>(
                    (BlockingQueue<JobContextLog>)new NonBlockingArrayQueue<JobContextLog>(logSize));
        }
        nodeSettingsService.addListener(new NodeSettingListener(jobsLog, lastLogSize));

        jobsLogIterableGetter = new JobsLogIterableGetter();
        jobsIterableGetter = new JobsIterableGetter();
        operationsIterableGetter = new OperationsIterableGetter();
    }

    /**
     * Indicates if statistics are gathered.
     * This result will change if the cluster settings is updated.
     */
    public boolean isEnabled() {
        return true;
    }

    /**
     * Track a job. If the job has finished {@link #jobFinished(io.crate.executor.Job, String)}
     * must be called.
     *
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void jobStarted(Job job, String statement) {
        if (!isEnabled()) {
            return;
        }
        jobsTable.put(job.id(), new JobContext(job.id(), statement, System.currentTimeMillis()));
    }

    /**
     * mark a job as finished.
     *
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void jobFinished(Job job, @Nullable String errorMessage) {
        if (!isEnabled()) {
            return;
        }
        JobContext jobContext = jobsTable.remove(job.id());
        BlockingQueue<JobContextLog> jobContextLogs = jobsLog.get();
        if (jobContextLogs != null) {
            jobContextLogs.offer(new JobContextLog(jobContext, errorMessage));
        }

    }

    public void operationStarted(UUID operationId, UUID jobId, String name) {
        if (isEnabled()) {
            operationsTable.put(
                    operationId,
                    new OperationContext(jobId, name, System.currentTimeMillis()));
        }
    }

    public void operationFinished(@Nullable UUID operationId) {
        if (operationId == null || !isEnabled()) {
            return;
        }
        operationsTable.remove(operationId);
    }


    public IterableGetter jobsGetter() {
        return jobsIterableGetter;
    }

    public IterableGetter jobsLogGetter() {
        return jobsLogIterableGetter;
    }

    public IterableGetter operationsGetter() {
        return operationsIterableGetter;
    }

    private class JobsLogIterableGetter implements IterableGetter {

        @Override
        public Iterable<?> getIterable() {
            return jobsLog.get();
        }
    }

    private class JobsIterableGetter implements IterableGetter {

        @Override
        public Iterable<?> getIterable() {
            return jobsTable.values();
        }
    }

    private class OperationsIterableGetter implements IterableGetter {

        @Override
        public Iterable<?> getIterable() {
            return operationsTable.values();
        }
    }

    private static class NodeSettingListener implements NodeSettingsService.Listener {

        private final AtomicReference<BlockingQueue<JobContextLog>> jobsLog;
        private final AtomicReference<Integer> lastLogSize;

        public NodeSettingListener(AtomicReference<BlockingQueue<JobContextLog>> jobsLog,
                                   AtomicReference<Integer> lastLogSize) {
            this.jobsLog = jobsLog;
            this.lastLogSize = lastLogSize;
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            int newLogSize = settings.getAsInt(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, 0);
            if (newLogSize != lastLogSize.get()) {
                lastLogSize.set(newLogSize);
                if (newLogSize == 0) {
                    jobsLog.set(NOOP_JOBS_LOG);
                } else {
                    jobsLog.set(new NonBlockingArrayQueue<JobContextLog>(newLogSize));
                }
            }

            // TODO: track isEnabled and if it changes to false remember to clear any current jobs/operations
            // as well as the history
        }
    }
}