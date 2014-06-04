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
import io.crate.operation.reference.sys.operation.OperationContextLog;
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
    private final AtomicReference<Integer> lastOperationsLogSize = new AtomicReference<>();
    private final AtomicReference<Integer> lastJobsLogSize = new AtomicReference<>();
    private final AtomicReference<BlockingQueue<JobContextLog>> jobsLog = new AtomicReference<>();
    private final AtomicReference<BlockingQueue<OperationContextLog>> operationsLog = new AtomicReference<>();
    private final static NoopQueue<OperationContextLog> NOOP_OPERATIONS_LOG = NoopQueue.instance();
    private final static NoopQueue<JobContextLog> NOOP_JOBS_LOG = NoopQueue.instance();

    private final JobsLogIterableGetter jobsLogIterableGetter;
    private final JobsIterableGetter jobsIterableGetter;
    private final OperationsIterableGetter operationsIterableGetter;
    private final OperationsLogIterableGetter operationsLogIterableGetter;


    public interface IterableGetter {
        public Iterable<?> getIterable();
    }

    @Inject
    public StatsTables(Settings settings, NodeSettingsService nodeSettingsService) {
        setOperationsLog(settings.getAsInt(ClusterSettingsExpression.SETTING_OPERATIONS_LOG_SIZE, 0));
        setJobsLog(settings.getAsInt(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, 0));
        nodeSettingsService.addListener(new NodeSettingListener());
        jobsLogIterableGetter = new JobsLogIterableGetter();
        jobsIterableGetter = new JobsIterableGetter();
        operationsIterableGetter = new OperationsIterableGetter();
        operationsLogIterableGetter = new OperationsLogIterableGetter();
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
        if (jobContext == null) {
            return;
        }
        BlockingQueue<JobContextLog> jobContextLogs = jobsLog.get();
        jobContextLogs.offer(new JobContextLog(jobContext, errorMessage));
    }

    public void operationStarted(UUID operationId, UUID jobId, String name) {
        if (isEnabled()) {
            operationsTable.put(
                    operationId,
                    new OperationContext(jobId, name, System.currentTimeMillis()));
        }
    }

    public void operationFinished(@Nullable UUID operationId, @Nullable String errorMessage) {
        if (operationId == null || !isEnabled()) {
            return;
        }
        OperationContext operationContext = operationsTable.remove(operationId);
        if (operationContext == null) {
            // this might be the case if the stats were disabled when the operation started but have
            // been enabled before the finish
            return;
        }
        BlockingQueue<OperationContextLog> operationContextLogs = operationsLog.get();
        operationContextLogs.offer(new OperationContextLog(operationContext, errorMessage));
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

    public IterableGetter operationsLogGetter() {
        return operationsLogIterableGetter;
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

    private class OperationsLogIterableGetter implements IterableGetter {

        @Override
        public Iterable<?> getIterable() {
            return operationsLog.get();
        }
    }

    private void setOperationsLog(int size) {
        lastOperationsLogSize.set(size);
        if (size == 0) {
            operationsLog.set(NOOP_OPERATIONS_LOG);
        } else {
            operationsLog.set(new NonBlockingArrayQueue<OperationContextLog>(size));
        }
    }

    private void setJobsLog(int size) {
        lastJobsLogSize.set(size);
        if (size == 0) {
            jobsLog.set(NOOP_JOBS_LOG);
        } else {
            jobsLog.set(new NonBlockingArrayQueue<JobContextLog>(size));
        }
    }

    private class NodeSettingListener implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            updateJobsLog(settings);
            updateOperationsLog(settings);
            // TODO: track isEnabled and if it changes to false remember to clear any current jobs/operations
            // as well as the history
        }

        private void updateOperationsLog(Settings settings) {
            int newSize = settings.getAsInt(ClusterSettingsExpression.SETTING_OPERATIONS_LOG_SIZE, 0);
            if (newSize != lastOperationsLogSize.get()) {
                setOperationsLog(newSize);
            }
        }

        private void updateJobsLog(Settings settings) {
            int newSize = settings.getAsInt(ClusterSettingsExpression.SETTING_JOBS_LOG_SIZE, 0);
            if (newSize != lastJobsLogSize.get()) {
                setJobsLog(newSize);
            }
        }
    }
}