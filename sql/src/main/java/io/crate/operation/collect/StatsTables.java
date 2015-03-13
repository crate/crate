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
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import jsr166e.LongAdder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stats tables that are globally available on each node and contain meta data of the cluster
 * like active jobs
 *
 * injected via guice instead of using static so that if two nodes run
 * in the same jvm the memoryTables aren't shared between the nodes.
 */
@ThreadSafe
public class StatsTables {

    protected final Map<UUID, JobContext> jobsTable = new ConcurrentHashMap<>();
    protected final Map<UUID, OperationContext> operationsTable = new ConcurrentHashMap<>();
    protected final AtomicReference<BlockingQueue<JobContextLog>> jobsLog = new AtomicReference<>();
    protected final AtomicReference<BlockingQueue<OperationContextLog>> operationsLog = new AtomicReference<>();
    private final static NoopQueue<OperationContextLog> NOOP_OPERATIONS_LOG = NoopQueue.instance();
    private final static NoopQueue<JobContextLog> NOOP_JOBS_LOG = NoopQueue.instance();

    private final JobsLogIterableGetter jobsLogIterableGetter;
    private final JobsIterableGetter jobsIterableGetter;
    private final OperationsIterableGetter operationsIterableGetter;
    private final OperationsLogIterableGetter operationsLogIterableGetter;

    protected final NodeSettingsService.Listener listener = new NodeSettingListener();
    protected volatile int lastOperationsLogSize;
    protected volatile int lastJobsLogSize;
    protected volatile boolean lastIsEnabled;

    private final LongAdder activeRequests = new LongAdder();

    public void activeRequestsInc() {
        activeRequests.increment();
    }

    public void activeRequestsDec() {
        activeRequests.decrement();
    }

    public long activeRequests() {
        return activeRequests.longValue();
    }

    public interface IterableGetter {
        public Iterable<?> getIterable();
    }

    @Inject
    public StatsTables(Settings settings, NodeSettingsService nodeSettingsService) {
        int operationsLogSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
        int jobsLogSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
        boolean isEnabled = CrateSettings.STATS_ENABLED.extract(settings);

        if (isEnabled) {
            setJobsLog(jobsLogSize);
            setOperationsLog(operationsLogSize);
        } else {
            setJobsLog(0);
            setOperationsLog(0);
        }

        lastOperationsLogSize = operationsLogSize;
        lastJobsLogSize = jobsLogSize;
        lastIsEnabled = isEnabled;

        nodeSettingsService.addListener(listener);
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
        return lastIsEnabled;
    }

    /**
     * Track a job. If the job has finished {@link #jobFinished(java.util.UUID, String)}
     * must be called.
     *
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void jobStarted(UUID jobId, String statement) {
        if (!isEnabled()) {
            return;
        }
        jobsTable.put(jobId, new JobContext(jobId, statement, System.currentTimeMillis()));
    }

    /**
     * mark a job as finished.
     *
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void jobFinished(UUID jobId, @Nullable String errorMessage) {
        if (!isEnabled()) {
            return;
        }
        JobContext jobContext = jobsTable.remove(jobId);
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
                    new OperationContext(operationId, jobId, name, System.currentTimeMillis()));
        }
    }

    public void operationFinished(@Nullable UUID operationId, @Nullable String errorMessage, long usedBytes) {
        if (operationId == null || !isEnabled()) {
            return;
        }
        OperationContext operationContext = operationsTable.remove(operationId);
        if (operationContext == null) {
            // this might be the case if the stats were disabled when the operation started but have
            // been enabled before the finish
            return;
        }
        operationContext.usedBytes = usedBytes;
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
        if (size == 0) {
            operationsLog.set(NOOP_OPERATIONS_LOG);
        } else {
            BlockingQueue<OperationContextLog> oldQ = operationsLog.get();
            NonBlockingArrayQueue<OperationContextLog> newQ = new NonBlockingArrayQueue<>(size);
            if (oldQ != null && oldQ.size() > 0) {
                oldQ.drainTo(newQ, size);
            }
            operationsLog.set(newQ);
        }
    }

    private void setJobsLog(int size) {
        if (size == 0) {
            jobsLog.set(NOOP_JOBS_LOG);
        } else {
            BlockingQueue<JobContextLog> oldQ = jobsLog.get();
            NonBlockingArrayQueue<JobContextLog> newQ = new NonBlockingArrayQueue<>(size);
            if (oldQ != null && oldQ.size() > 0) {
                oldQ.drainTo(newQ, size);
            }
            jobsLog.set(newQ);
        }
    }

    private class NodeSettingListener implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            boolean wasEnabled = lastIsEnabled;
            boolean becomesEnabled = CrateSettings.STATS_ENABLED.extract(settings);

            if (wasEnabled && becomesEnabled) {
                int opSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
                if (opSize != lastOperationsLogSize) {
                    lastOperationsLogSize = opSize;
                    setOperationsLog(opSize);
                }

                int jobSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
                if (jobSize != lastJobsLogSize) {
                    lastJobsLogSize = jobSize;
                    setJobsLog(jobSize);
                }

            } else if (wasEnabled) { // !becomesEnabled
                setOperationsLog(0);
                setJobsLog(0);
                lastIsEnabled = false;

                lastOperationsLogSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
                lastJobsLogSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
            } else if (becomesEnabled) { // !wasEnabled
                lastIsEnabled = true;

                // queue sizes was zero before so we have to change it
                int opSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
                lastOperationsLogSize = opSize;
                setOperationsLog(opSize);

                int jobSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
                lastJobsLogSize = jobSize;
                setJobsLog(jobSize);
            }
        }
    }
}