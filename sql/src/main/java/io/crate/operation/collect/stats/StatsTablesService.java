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
import io.crate.breaker.SizeEstimator;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.ContextLog;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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
public class StatsTablesService extends AbstractLifecycleComponent implements Provider<StatsTables> {

    private final ScheduledExecutorService scheduler;
    private final CrateCircuitBreakerService breakerService;

    private StatsTables statsTables;
    LogSink<JobContextLog> jobsLogSink = NoopLogSink.instance();
    LogSink<OperationContextLog> operationsLogSink = NoopLogSink.instance();

    private final boolean initialIsEnabled;
    private final int initialJobsLogSize;
    private final TimeValue initialJobsLogExpiration;
    private final int initialOperationsLogSize;
    private final TimeValue initialOperationsLogExpiration;

    volatile boolean lastIsEnabled;
    volatile int lastJobsLogSize;
    volatile TimeValue lastJobsLogExpiration;
    volatile int lastOperationsLogSize;
    volatile TimeValue lastOperationsLogExpiration;

    static final JobContextLogSizeEstimator JOB_CONTEXT_LOG_ESTIMATOR = new JobContextLogSizeEstimator();
    static final OperationContextLogSizeEstimator OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR = new OperationContextLogSizeEstimator();

    @Inject
    public StatsTablesService(Settings settings,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              CrateCircuitBreakerService breakerService) {
        this(settings, clusterService, threadPool.scheduler(), breakerService);
    }

    @VisibleForTesting
    StatsTablesService(Settings settings,
                       ClusterService clusterService,
                       ScheduledExecutorService scheduledExecutorService,
                       CrateCircuitBreakerService breakerService) {
        super(settings);
        scheduler = scheduledExecutorService;
        this.breakerService = breakerService;
        //nodeSettingsService.addListener(listener);

        int jobsLogSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
        TimeValue jobsLogExpiration = CrateSettings.STATS_JOBS_LOG_EXPIRATION.extractTimeValue(settings);
        int operationsLogSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
        TimeValue operationsLogExpiration = CrateSettings.STATS_OPERATIONS_LOG_EXPIRATION.extractTimeValue(settings);

        boolean isEnabled = CrateSettings.STATS_ENABLED.extract(settings);

        initialIsEnabled = isEnabled;
        initialJobsLogSize = jobsLogSize;
        initialJobsLogExpiration = jobsLogExpiration;
        initialOperationsLogSize = operationsLogSize;
        initialOperationsLogExpiration = operationsLogExpiration;

        lastIsEnabled = isEnabled;
        lastJobsLogSize = jobsLogSize;
        lastJobsLogExpiration = jobsLogExpiration;
        lastOperationsLogSize = operationsLogSize;
        lastOperationsLogExpiration = operationsLogExpiration;

        statsTables = new StatsTables(this::isEnabled);
        if (isEnabled()) {
            setJobsLogSink(lastJobsLogSize, lastJobsLogExpiration);
            setOperationsLogSink(lastOperationsLogSize, lastOperationsLogExpiration);
        } else {
            setJobsLogSink(0, TimeValue.timeValueSeconds(0L));
            setOperationsLogSink(0, TimeValue.timeValueSeconds(0L));
        }

        /*
        this.breakerService.addBreakerChangeListener(CrateCircuitBreakerService.JOBS_LOG,
            (CircuitBreaker breaker) -> setJobsLogSink(lastJobsLogSize, lastJobsLogExpiration)
        );
        this.breakerService.addBreakerChangeListener(CrateCircuitBreakerService.OPERATIONS_LOG,
            (CircuitBreaker breaker) -> setOperationsLogSink(lastOperationsLogSize, lastOperationsLogExpiration)
        );
        */
    }

    private void setJobsLogSink(int size, TimeValue expiration) {
        LogSink<JobContextLog> newSink = createSink(size, expiration, JOB_CONTEXT_LOG_ESTIMATOR,
            CrateCircuitBreakerService.JOBS_LOG);
        LogSink<JobContextLog> oldSink = jobsLogSink;
        newSink.addAll(oldSink);
        jobsLogSink = newSink;
        statsTables.updateJobsLog(jobsLogSink);
        oldSink.close();
    }

    /**
     * specifies scheduler interval that depends on the provided expiration
     *
     * min = 1s (1_000ms)
     * max = 1d (86_400_000ms)
     * min <= expiration/10 <= max
     *
     * @param expiration the specified expiration time
     * @return the scheduler interval in ms
     */
    static long clearInterval(TimeValue expiration) {
        return Long.min(Long.max(1_000L, expiration.getMillis() / 10), 86_400_000L);
    }

    private <E extends ContextLog> LogSink<E> createSink(int size, TimeValue expiration, SizeEstimator<E> sizeEstimator, String breaker) {
        Queue<E> q;
        long expirationMillis = expiration.getMillis();
        final Runnable onClose;
        if (size == 0 && expirationMillis == 0) {
            return NoopLogSink.instance();
        } else if (expirationMillis > 0) {
            q = new ConcurrentLinkedDeque<>();
            TimeExpiring lbTimeExpiring = new TimeExpiring(clearInterval(expiration));
            ScheduledFuture<?> scheduledFuture = lbTimeExpiring.registerTruncateTask(q, scheduler, expiration);
            onClose = () -> scheduledFuture.cancel(false);
        } else {
            q = new BlockingEvictingQueue<>(size);
            onClose = () -> {};
        }

        RamAccountingQueue<E> accountingQueue = new RamAccountingQueue<>(q, breakerService.getBreaker(breaker), sizeEstimator);
        return new QueueSink<>(accountingQueue, () -> {
            accountingQueue.close();
            onClose.run();
        });
    }

    private void setOperationsLogSink(int size, TimeValue expiration) {
        LogSink<OperationContextLog> newSink = createSink(size, expiration, OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR,
            CrateCircuitBreakerService.OPERATIONS_LOG);
        LogSink<OperationContextLog> oldSink = operationsLogSink;
        newSink.addAll(oldSink);
        operationsLogSink = newSink;
        statsTables.updateOperationsLog(operationsLogSink);
        oldSink.close();
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

    private TimeValue extractOperationsLogExpiration(Settings settings) {
        return CrateSettings.STATS_OPERATIONS_LOG_EXPIRATION.extractTimeValue(settings, initialOperationsLogExpiration);
    }

    public StatsTables statsTables() {
        return statsTables;
    }

    @Override
    public StatsTables get() {
        return statsTables();
    }

    /*
    private class NodeSettingListener implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            boolean wasEnabled = lastIsEnabled;
            boolean becomesEnabled = extractIsEnabled(settings);

            if (wasEnabled && becomesEnabled) {
                int opSize = extractOperationsLogSize(settings);
                TimeValue opExpiration = extractOperationsLogExpiration(settings);
                if (opSize != lastOperationsLogSize || !opExpiration.equals(lastOperationsLogExpiration)) {
                    lastOperationsLogSize = opSize;
                    lastOperationsLogExpiration = opExpiration;
                    setOperationsLogSink(opSize, opExpiration);
                }

                int jobSize = extractJobsLogSize(settings);
                TimeValue jobExpiration = extractJobsLogExpiration(settings);
                if (jobSize != lastJobsLogSize || !jobExpiration.equals(lastJobsLogExpiration)) {
                    lastJobsLogSize = jobSize;
                    lastJobsLogExpiration = jobExpiration;
                    setJobsLogSink(jobSize, jobExpiration);
                }

            } else if (wasEnabled) { // !becomesEnabled
                setOperationsLogSink(0, TimeValue.timeValueSeconds(0L));
                setJobsLogSink(0, TimeValue.timeValueSeconds(0L));
                lastIsEnabled = false;

                lastOperationsLogSize = extractOperationsLogSize(settings);
                lastJobsLogSize = extractJobsLogSize(settings);
            } else if (becomesEnabled) { // !wasEnabled
                lastIsEnabled = true;

                // queue sizes was zero before so we have to change it
                int opSize = extractOperationsLogSize(settings);
                TimeValue opExpiration = extractOperationsLogExpiration(settings);
                lastOperationsLogSize = opSize;
                lastOperationsLogExpiration = opExpiration;
                setOperationsLogSink(opSize, opExpiration);

                int jobSize = extractJobsLogSize(settings);
                TimeValue jobExpiration = extractJobsLogExpiration(settings);
                lastJobsLogSize = jobSize;
                lastJobsLogExpiration = jobExpiration;
                setJobsLogSink(jobSize, jobExpiration);
            }
        }
    }
    */
}
