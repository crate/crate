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

package io.crate.execution.engine.collect.stats;

import io.crate.user.User;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.expression.reference.sys.operation.OperationContext;
import io.crate.expression.reference.sys.operation.OperationContextLog;
import io.crate.metadata.sys.ClassifiedMetrics;
import io.crate.metadata.sys.MetricsView;
import io.crate.planner.operators.StatementClassifier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static io.crate.planner.Plan.StatementType.UNDEFINED;


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

    record OperationId(int operationId, UUID jobId) {}

    private final Map<UUID, JobContext> jobsTable = new ConcurrentHashMap<>();
    private final Map<OperationId, OperationContext> operationsTable = new ConcurrentHashMap<>();

    private LogSink<JobContextLog> jobsLog = NoopLogSink.instance();
    private LogSink<OperationContextLog> operationsLog = NoopLogSink.instance();

    private final StampedLock jobsLogLock = new StampedLock();
    private final StampedLock operationsLogRWLock = new StampedLock();

    private final LongAdder activeRequests = new LongAdder();
    private final BooleanSupplier enabled;
    private final ClassifiedMetrics classifiedMetrics = new ClassifiedMetrics();

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
     * Track a job. If the job has finished {@link #logExecutionEnd(java.util.UUID, String)}
     * must be called.
     * <p>
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void logExecutionStart(UUID jobId, String statement, User user, StatementClassifier.Classification classification) {
        activeRequests.increment();
        if (!isEnabled()) {
            return;
        }
        jobsTable.put(jobId, new JobContext(jobId, statement, System.currentTimeMillis(), user, classification));
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
        JobContextLog jobContextLog = new JobContextLog(jobContext, errorMessage);
        recordMetrics(jobContextLog);
        long stamp = jobsLogLock.readLock();
        try {
            jobsLog.add(jobContextLog);
        } finally {
            jobsLogLock.unlockRead(stamp);
        }
    }

    private void recordMetrics(JobContextLog log) {
        StatementClassifier.Classification classification = log.classification();
        assert classification != null : "A job must have a classification";
        if (log.errorMessage() == null) {
            classifiedMetrics.recordValue(classification, log.ended() - log.started());
        } else {
            classifiedMetrics.recordFailedExecution(classification, log.ended() - log.started());
        }
    }

    /**
     * Create a entry into `sys.jobs_log`
     * This method can be used instead of {@link #logExecutionEnd(UUID, String)} if there was no {@link #logExecutionStart(UUID, String, User, StatementClassifier.Classification)}
     * Call because an error happened during parse, analysis or plan.
     * <p>
     * {@link #logExecutionStart(UUID, String, User, StatementClassifier.Classification)} is only called after a Plan has been created and execution starts.
     */
    public void logPreExecutionFailure(UUID jobId, String stmt, String errorMessage, User user) {
        JobContextLog jobContextLog = new JobContextLog(
            new JobContext(jobId, stmt, System.currentTimeMillis(), user, new StatementClassifier.Classification(UNDEFINED)), errorMessage);
        long stamp = jobsLogLock.readLock();
        try {
            jobsLog.add(jobContextLog);
        } finally {
            jobsLogLock.unlockRead(stamp);
        }
        recordMetrics(jobContextLog);
    }

    public void operationStarted(int operationId, UUID jobId, String name, LongSupplier bytesUsed) {
        if (isEnabled()) {
            operationsTable.put(
                new OperationId(operationId, jobId),
                new OperationContext(operationId, jobId, name, System.currentTimeMillis(), bytesUsed));
        }
    }

    public Iterable<MetricsView> metrics() {
        return classifiedMetrics;
    }

    public void operationFinished(int operationId, UUID jobId, @Nullable String errorMessage) {
        if (!isEnabled()) {
            return;
        }
        OperationContext operationContext = operationsTable.remove(new OperationId(operationId, jobId));
        if (operationContext == null) {
            // this might be the case if the stats were disabled when the operation started but have
            // been enabled before the finish
            return;
        }
        OperationContextLog operationContextLog = new OperationContextLog(operationContext, errorMessage);
        long stamp = operationsLogRWLock.readLock();
        try {
            operationsLog.add(operationContextLog);
        } finally {
            operationsLogRWLock.unlockRead(stamp);
        }
    }

    public Iterable<JobContext> activeJobs() {
        return jobsTable.values();
    }

    public Iterable<JobContextLog> jobsLog() {
        long stamp = jobsLogLock.readLock();
        try {
            return jobsLog;
        } finally {
            jobsLogLock.unlockRead(stamp);
        }
    }

    public Iterable<OperationContext> activeOperations() {
        return operationsTable.values();
    }

    public Iterable<OperationContextLog> operationsLog() {
        long stamp = operationsLogRWLock.readLock();
        try {
            return operationsLog;
        } finally {
            operationsLogRWLock.unlockRead(stamp);
        }
    }

    public long activeRequests() {
        return activeRequests.longValue();
    }

    void updateOperationsLog(LogSink<OperationContextLog> sink) {
        long stamp = operationsLogRWLock.writeLock();
        try {
            sink.addAll(operationsLog);
            operationsLog.close();
            operationsLog = sink;
        } finally {
            operationsLogRWLock.unlockWrite(stamp);
        }
    }

    void updateJobsLog(LogSink<JobContextLog> sink) {
        long stamp = jobsLogLock.writeLock();
        try {
            sink.addAll(jobsLog);
            jobsLog.close();
            jobsLog = sink;
        } finally {
            jobsLogLock.unlockWrite(stamp);
        }
    }

    void resetMetrics() {
        classifiedMetrics.reset();
    }

    public void close() {
        jobsLog.close();
        operationsLog.close();
    }
}
