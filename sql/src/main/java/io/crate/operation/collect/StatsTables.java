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

import com.google.common.base.Supplier;
import com.twitter.jsr166e.LongAdder;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.core.collections.NoopQueue;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Queue;
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
@Singleton
public class StatsTables {

    private final static BlockingQueue<OperationContextLog> NOOP_OPERATIONS_LOG = NoopQueue.instance();
    private final static BlockingQueue<JobContextLog> NOOP_JOBS_LOG = NoopQueue.instance();

    protected final Map<UUID, JobContext> jobsTable = new ConcurrentHashMap<>();
    protected final Map<Integer, OperationContext> operationsTable = new ConcurrentHashMap<>();
    protected final AtomicReference<BlockingQueue<JobContextLog>> jobsLog = new AtomicReference<>(NOOP_JOBS_LOG);
    protected final AtomicReference<BlockingQueue<OperationContextLog>> operationsLog = new AtomicReference<>(NOOP_OPERATIONS_LOG);

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
        Queue<JobContextLog> jobContextLogs = jobsLog.get();
        jobContextLogs.offer(new JobContextLog(jobContext, errorMessage));
    }

    public void operationStarted(int operationId, UUID jobId, String name) {
        if (isEnabled()) {
            operationsTable.put(
                    operationId,
                    new OperationContext(operationId, jobId, name, System.currentTimeMillis()));
        }
    }

    public void operationFinished(@Nullable Integer operationId, @Nullable String errorMessage, long usedBytes) {
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
        Queue<OperationContextLog> operationContextLogs = operationsLog.get();
        operationContextLogs.offer(new OperationContextLog(operationContext, errorMessage));
    }


    public Supplier<Iterable<?>> jobsGetter() {
        return jobsIterableGetter;
    }

    public Supplier<Iterable<?>> jobsLogGetter() {
        return jobsLogIterableGetter;
    }

    public Supplier<Iterable<?>> operationsGetter() {
        return operationsIterableGetter;
    }

    public Supplier<Iterable<?>> operationsLogGetter() {
        return operationsLogIterableGetter;
    }

    private class JobsLogIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return jobsLog.get();
        }
    }

    private class JobsIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return jobsTable.values();
        }
    }

    private class OperationsIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return operationsTable.values();
        }
    }

    private class OperationsLogIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return operationsLog.get();
        }
    }

    private void setOperationsLog(int size) {
        if (size == 0) {
            operationsLog.set(NOOP_OPERATIONS_LOG);
        } else {
            Queue<OperationContextLog> oldQ = operationsLog.get();
            BlockingEvictingQueue<OperationContextLog> newQ = new BlockingEvictingQueue<>(size);
            newQ.addAll(oldQ);
            operationsLog.set(newQ);
        }
    }

    private void setJobsLog(int size) {
        if (size == 0) {
            jobsLog.set(NOOP_JOBS_LOG);
        } else {
            Queue<JobContextLog> oldQ = jobsLog.get();
            BlockingEvictingQueue<JobContextLog> newQ = new BlockingEvictingQueue<>(size);
            newQ.addAll(oldQ);
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