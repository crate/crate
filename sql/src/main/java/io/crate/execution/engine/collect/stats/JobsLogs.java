/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect.stats;

import io.crate.auth.user.User;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.expression.reference.sys.operation.OperationContext;
import io.crate.expression.reference.sys.operation.OperationContextLog;
import io.crate.metadata.sys.SysMetricsTableInfo;
import org.HdrHistogram.ConcurrentHistogram;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

import static java.util.Collections.singletonList;

/**
 * JobsLogs is responsible for adding jobs and operations of that node.
 * It also provides the functionality to expose that data for system tables,
 * such as sys.jobs, sys.jobs_log, sys.operations and sys.operations_log;
 * <p>
 * The data is exposed via the properties
 *
 *   - {@link #activeJobs()}
 *   - {@link #jobsLog()} ()}
 *   - {@link #activeOperations()} ()}
 *   - {@link #operationsLog()} ()}
 *
 * Note that on configuration updates (E.g.: resizing of jobs-log size, etc.) the Iterable instances previously returned
 * from the properties may become obsolete.
 * So the Iterable instances shouldn't be hold onto.
 * Instead the Iterable should be re-retrieved each time the data is processed.
 */
@ThreadSafe
public class JobsLogs {

    private final Map<UUID, JobContext> jobsTable = new ConcurrentHashMap<>();
    private final Map<Tuple<Integer, UUID>, OperationContext> operationsTable = new ConcurrentHashMap<>();

    final AtomicReference<LogSink<JobContextLog>> jobsLog = new AtomicReference<>(NoopLogSink.instance());
    final AtomicReference<LogSink<OperationContextLog>> operationsLog = new AtomicReference<>(NoopLogSink.instance());

    private final LongAdder activeRequests = new LongAdder();
    private final BooleanSupplier enabled;
    private final ConcurrentHistogram histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toMillis(10), 3);

    public JobsLogs(BooleanSupplier enabled) {
        this.enabled = enabled;
    }

    /**
     * Indicates if statistics are gathered.
     * This result will change if the cluster settings is updated.
     */
    private boolean isEnabled() {
        return enabled.getAsBoolean();
    }

    /**
     * Generate a unique ID for an operation based on jobId and operationId.
     */
    private static Tuple<Integer, UUID> uniqueOperationId(int operationId, UUID jobId) {
        return Tuple.tuple(operationId, jobId);
    }

    /**
     * Track a job. If the job has finished {@link #logExecutionEnd(java.util.UUID, String)}
     * must be called.
     * <p>
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void logExecutionStart(UUID jobId, String statement, User user) {
        activeRequests.increment();
        if (!isEnabled()) {
            return;
        }
        jobsTable.put(jobId, new JobContext(jobId, statement, System.currentTimeMillis(), user));
    }

    /**
     * mark a job as finished.
     * <p>
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void logExecutionEnd(UUID jobId, @Nullable String errorMessage) {
        activeRequests.decrement();
        JobContext jobContext = jobsTable.remove(jobId);
        if (!isEnabled() || jobContext == null) {
            return;
        }
        LogSink<JobContextLog> jobContextLogs = jobsLog.get();
        JobContextLog contextLog = new JobContextLog(jobContext, errorMessage);
        histogram.recordValue(contextLog.ended() - contextLog.started());
        jobContextLogs.add(contextLog);
    }

    /**
     * Create a entry into `sys.jobs_log`
     * This method can be used instead of {@link #logExecutionEnd(UUID, String)} if there was no {@link #logExecutionStart(UUID, String, User)}
     * Call because an error happened during parse, analysis or plan.
     * <p>
     * {@link #logExecutionStart(UUID, String, User)} is only called after a Plan has been created and execution starts.
     */
    public void logPreExecutionFailure(UUID jobId, String stmt, String errorMessage, @Nullable User user) {
        LogSink<JobContextLog> jobContextLogs = jobsLog.get();
        JobContext jobContext = new JobContext(jobId, stmt, System.currentTimeMillis(), user);
        jobContextLogs.add(new JobContextLog(jobContext, errorMessage));
    }

    public void operationStarted(int operationId, UUID jobId, String name) {
        if (isEnabled()) {
            operationsTable.put(
                uniqueOperationId(operationId, jobId),
                new OperationContext(operationId, jobId, name, System.currentTimeMillis()));
        }
    }

    public Iterable<SysMetricsTableInfo.ClassifiedHist> metrics() {
        return singletonList(new SysMetricsTableInfo.ClassifiedHist(histogram.copy(), "total"));
    }

    public void operationFinished(int operationId, UUID jobId, @Nullable String errorMessage, long usedBytes) {
        if (!isEnabled()) {
            return;
        }
        OperationContext operationContext = operationsTable.remove(uniqueOperationId(operationId, jobId));
        if (operationContext == null) {
            // this might be the case if the stats were disabled when the operation started but have
            // been enabled before the finish
            return;
        }
        operationContext.usedBytes = usedBytes;
        LogSink<OperationContextLog> operationContextLogs = operationsLog.get();
        operationContextLogs.add(new OperationContextLog(operationContext, errorMessage));
    }

    public Iterable<JobContext> activeJobs() {
        return jobsTable.values();
    }

    public Iterable<JobContextLog> jobsLog() {
        return jobsLog.get();
    }

    public Iterable<OperationContext> activeOperations() {
        return operationsTable.values();
    }

    public Iterable<OperationContextLog> operationsLog() {
        return operationsLog.get();
    }

    public long activeRequests() {
        return activeRequests.longValue();
    }

    void updateOperationsLog(LogSink<OperationContextLog> sink) {
        operationsLog.set(sink);
    }

    void updateJobsLog(LogSink<JobContextLog> sink) {
        jobsLog.set(sink);
    }

}
