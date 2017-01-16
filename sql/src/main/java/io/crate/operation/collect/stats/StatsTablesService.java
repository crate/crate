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

package io.crate.operation.collect.stats;

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.JobContextLogSizeEstimator;
import io.crate.breaker.OperationContextLogSizeEstimator;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * The StatsTablesService is available on each node and holds the meta data of the cluster, such as active jobs and operations.
 * This data is exposed via the StatsTables.
 * <p>
 * It is injected via guice instead of using static so that if two nodes run
 * in the same jvm the memoryTables aren't shared between the nodes.
 */
@Singleton
public class StatsTablesService extends AbstractLifecycleComponent<StatsTablesService> implements Provider<StatsTables> {

    private static final ESLogger LOGGER = Loggers.getLogger(StatsTablesService.class);

    private final RamAccountingContext ramAccountingContext;
    protected final NodeSettingsService.Listener listener = new NodeSettingListener();
    private final ScheduledExecutorService scheduler;

    private StatsTables statsTables;
    LogSink<JobContextLog> jobsLogSink = NoopLogSink.instance();
    LogSink<OperationContextLog> operationsLogSink = NoopLogSink.instance();

    private final int initialOperationsLogSize;
    private final int initialJobsLogSize;
    private final TimeValue initialJobsLogExpiration;
    private final boolean initialIsEnabled;

    volatile int lastOperationsLogSize;
    volatile int lastJobsLogSize;
    volatile TimeValue lastJobsLogExpiration;
    private volatile boolean lastIsEnabled;

    static final JobContextLogSizeEstimator JOB_CONTEXT_LOG_ESTIMATOR = new JobContextLogSizeEstimator();
    static final OperationContextLogSizeEstimator OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR = new OperationContextLogSizeEstimator();

    @Inject
    public StatsTablesService(Settings settings, NodeSettingsService nodeSettingsService, ThreadPool threadPool, CrateCircuitBreakerService breakerService) {
        this(settings, nodeSettingsService, threadPool.scheduler(), breakerService);
    }

    @VisibleForTesting
    StatsTablesService(Settings settings, NodeSettingsService nodeSettingsService, ScheduledExecutorService scheduledExecutorService, CrateCircuitBreakerService breakerService) {
        super(settings);
        scheduler = scheduledExecutorService;
        CircuitBreaker circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.LOGS);
        ramAccountingContext = new RamAccountingContext("statsTablesContext", circuitBreaker);
        nodeSettingsService.addListener(listener);

        int operationsLogSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
        int jobsLogSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
        TimeValue jobsLogExpiration = CrateSettings.STATS_JOBS_LOG_EXPIRATION.extractTimeValue(settings);

        boolean isEnabled = CrateSettings.STATS_ENABLED.extract(settings);

        initialIsEnabled = isEnabled;
        initialJobsLogSize = jobsLogSize;
        initialJobsLogExpiration = jobsLogExpiration;
        initialOperationsLogSize = operationsLogSize;
        lastOperationsLogSize = operationsLogSize;
        lastJobsLogSize = jobsLogSize;
        lastJobsLogExpiration = jobsLogExpiration;
        lastIsEnabled = isEnabled;

        statsTables = new StatsTables(this::isEnabled);
        if (isEnabled()) {
            setJobsLogSink(lastJobsLogSize, lastJobsLogExpiration);
            setOperationsLogSink(lastOperationsLogSize);
        } else {
            setJobsLogSink(0, TimeValue.timeValueSeconds(0L));
            setOperationsLogSink(0);
        }
    }

    private void setJobsLogSink(int size, TimeValue expiration) {
        LogSink<JobContextLog> newSink = createSink(size, expiration);
        newSink.addAll(jobsLogSink);
        jobsLogSink.close();
        jobsLogSink = newSink;
        statsTables.updateJobsLog(jobsLogSink);
    }

    private LogSink<JobContextLog> createSink(int size, TimeValue expiration) {
        Queue<JobContextLog> q;
        long expirationMillis = expiration.getMillis();
        if (size == 0) {
            if (expirationMillis == 0) {
                return NoopLogSink.instance();
            }
            q = new ConcurrentLinkedDeque<>();
        } else {
            q = new BlockingEvictingQueue<>(size);
        }

        final Runnable onClose;
        if (expirationMillis > 0) {
            ScheduledFuture<?> scheduledFuture = TimeExpiring.instance().registerTruncateTask(q, scheduler, expiration);
            onClose = () -> scheduledFuture.cancel(false);
        } else {
            onClose = () -> {};
        }
        RamAccountingQueue<JobContextLog> accountingQueue =
            new RamAccountingQueue<>(q, ramAccountingContext, JOB_CONTEXT_LOG_ESTIMATOR);

        return new QueueSink<>(accountingQueue, () -> { accountingQueue.close(); onClose.run(); });
    }

    private void setOperationsLogSink(int size) {
        LogSink<OperationContextLog> newSink = createSink(size);
        newSink.addAll(operationsLogSink);
        operationsLogSink.close();
        operationsLogSink = newSink;
        statsTables.updateOperationsLog(operationsLogSink);
    }

    private LogSink<OperationContextLog> createSink(int size) {
        if (size == 0) {
            return NoopLogSink.instance();
        }
        RamAccountingQueue<OperationContextLog> accountingQueue =
            new RamAccountingQueue<>(new BlockingEvictingQueue<>(size), ramAccountingContext, OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR);
        return new QueueSink<>(accountingQueue, accountingQueue::close);
    }

    /**
     * Indicates if statistics are gathered.
     * This result will change if the cluster settings is updated.
     */
    public boolean isEnabled() {
        return lastIsEnabled;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        jobsLogSink.close();
        operationsLogSink.close();
    }

    private Integer extractJobsLogSize(Settings settings) {
        return CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings, initialJobsLogSize);
    }

    private TimeValue extractJobsLogExpiration(Settings settings) {
        return CrateSettings.STATS_JOBS_LOG_EXPIRATION.extractTimeValue(settings, initialJobsLogExpiration);
    }

    private Boolean extractIsEnabled(Settings settings) {
        return CrateSettings.STATS_ENABLED.extract(settings, initialIsEnabled);
    }

    private Integer extractOperationsLogSize(Settings settings) {
        return CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings, initialOperationsLogSize);
    }

    public StatsTables statsTables() {
        return statsTables;
    }

    @Override
    public StatsTables get() {
        return statsTables();
    }

    private class NodeSettingListener implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            boolean wasEnabled = lastIsEnabled;
            boolean becomesEnabled = extractIsEnabled(settings);

            if (wasEnabled && becomesEnabled) {
                int opSize = extractOperationsLogSize(settings);
                if (opSize != lastOperationsLogSize) {
                    lastOperationsLogSize = opSize;
                    setOperationsLogSink(opSize);
                }

                int jobSize = extractJobsLogSize(settings);
                TimeValue jobExpiration = extractJobsLogExpiration(settings);
                if (jobSize != lastJobsLogSize || jobExpiration != lastJobsLogExpiration) {
                    lastJobsLogSize = jobSize;
                    lastJobsLogExpiration = jobExpiration;
                    setJobsLogSink(jobSize, jobExpiration);
                }

            } else if (wasEnabled) { // !becomesEnabled
                setOperationsLogSink(0);
                setJobsLogSink(0, TimeValue.timeValueSeconds(0));
                lastIsEnabled = false;

                lastOperationsLogSize = extractOperationsLogSize(settings);
                lastJobsLogSize = extractJobsLogSize(settings);
            } else if (becomesEnabled) { // !wasEnabled
                lastIsEnabled = true;

                // queue sizes was zero before so we have to change it
                int opSize = extractOperationsLogSize(settings);
                lastOperationsLogSize = opSize;
                setOperationsLogSink(opSize);

                int jobSize = extractJobsLogSize(settings);
                TimeValue jobExpiration = extractJobsLogExpiration(settings);
                lastJobsLogSize = jobSize;
                lastJobsLogExpiration = jobExpiration;
                setJobsLogSink(jobSize, jobExpiration);
            }
        }
    }

}
