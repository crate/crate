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

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.JobContextLogSizeEstimator;
import io.crate.breaker.OperationContextLogSizeEstimator;
import io.crate.breaker.SizeEstimator;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.expression.reference.sys.job.ContextLog;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.expression.reference.sys.operation.OperationContextLog;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * The JobsLogService is available on each node and holds the meta data of the cluster, such as active jobs and operations.
 * This data is exposed via the JobsLogs.
 * <p>
 * It is injected via guice instead of using static so that if two nodes run
 * in the same jvm the memoryTables aren't shared between the nodes.
 */
@Singleton
public class JobsLogService extends AbstractLifecycleComponent implements Provider<JobsLogs> {

    public static final CrateSetting<Boolean> STATS_ENABLED_SETTING = CrateSetting.of(Setting.boolSetting(
        "stats.enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.BOOLEAN);
    public static final CrateSetting<Integer> STATS_JOBS_LOG_SIZE_SETTING = CrateSetting.of(Setting.intSetting(
        "stats.jobs_log_size", 10_000, 0, Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.INTEGER);
    public static final CrateSetting<TimeValue> STATS_JOBS_LOG_EXPIRATION_SETTING = CrateSetting.of(Setting.timeSetting(
        "stats.jobs_log_expiration", TimeValue.timeValueSeconds(0L), Setting.Property.NodeScope, Setting.Property.Dynamic),
        DataTypes.STRING);
    public static final CrateSetting<Integer> STATS_OPERATIONS_LOG_SIZE_SETTING = CrateSetting.of(Setting.intSetting(
        "stats.operations_log_size", 10_000, 0, Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.INTEGER);
    public static final CrateSetting<TimeValue> STATS_OPERATIONS_LOG_EXPIRATION_SETTING = CrateSetting.of(Setting.timeSetting(
        "stats.operations_log_expiration", TimeValue.timeValueSeconds(0L), Setting.Property.NodeScope, Setting.Property.Dynamic),
        DataTypes.STRING);

    private static final JobContextLogSizeEstimator JOB_CONTEXT_LOG_ESTIMATOR = new JobContextLogSizeEstimator();
    private static final OperationContextLogSizeEstimator OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR = new OperationContextLogSizeEstimator();

    private final ScheduledExecutorService scheduler;
    private final CrateCircuitBreakerService breakerService;

    private JobsLogs jobsLogs;
    LogSink<JobContextLog> jobsLogSink = NoopLogSink.instance();
    LogSink<OperationContextLog> operationsLogSink = NoopLogSink.instance();

    private volatile boolean isEnabled;
    volatile int jobsLogSize;
    volatile TimeValue jobsLogExpiration;
    volatile int operationsLogSize;
    volatile TimeValue operationsLogExpiration;

    @Inject
    public JobsLogService(Settings settings,
                              ClusterSettings clusterSettings,
                              ThreadPool threadPool,
                              CrateCircuitBreakerService breakerService) {
        this(settings, clusterSettings, threadPool.scheduler(), breakerService);
    }

    @VisibleForTesting
    JobsLogService(Settings settings,
                       ClusterSettings clusterSettings,
                       ScheduledExecutorService scheduledExecutorService,
                       CrateCircuitBreakerService breakerService) {
        super(settings);
        scheduler = scheduledExecutorService;
        this.breakerService = breakerService;

        isEnabled = STATS_ENABLED_SETTING.setting().get(settings);
        jobsLogs = new JobsLogs(this::isEnabled);
        setJobsLogSink(
            STATS_JOBS_LOG_SIZE_SETTING.setting().get(settings), STATS_JOBS_LOG_EXPIRATION_SETTING.setting().get(settings));
        setOperationsLogSink(
            STATS_OPERATIONS_LOG_SIZE_SETTING.setting().get(settings), STATS_OPERATIONS_LOG_EXPIRATION_SETTING.setting().get(settings));

        clusterSettings.addSettingsUpdateConsumer(STATS_ENABLED_SETTING.setting(), this::setStatsEnabled);
        clusterSettings.addSettingsUpdateConsumer(
            STATS_JOBS_LOG_SIZE_SETTING.setting(), STATS_JOBS_LOG_EXPIRATION_SETTING.setting(), this::setJobsLogSink);
        clusterSettings.addSettingsUpdateConsumer(
            STATS_OPERATIONS_LOG_SIZE_SETTING.setting(), STATS_OPERATIONS_LOG_EXPIRATION_SETTING.setting(), this::setOperationsLogSink);
    }

    private void setJobsLogSink(int size, TimeValue expiration) {
        jobsLogSize = size;
        jobsLogExpiration = expiration;
        if (!isEnabled) {
            return;
        }
        updateJobSink(size, expiration);
    }

    private void updateJobSink(int size, TimeValue expiration) {
        LogSink<JobContextLog> newSink = createSink(
            size, expiration, JOB_CONTEXT_LOG_ESTIMATOR, CrateCircuitBreakerService.JOBS_LOG);
        LogSink<JobContextLog> oldSink = jobsLogSink;
        newSink.addAll(oldSink);
        jobsLogSink = newSink;
        jobsLogs.updateJobsLog(jobsLogSink);
        oldSink.close();
    }

    /**
     * specifies scheduler interval that depends on the provided expiration
     * <p>
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
            onClose = () -> {
            };
        }

        RamAccountingQueue<E> accountingQueue = new RamAccountingQueue<>(q, breakerService.getBreaker(breaker), sizeEstimator);
        return new QueueSink<>(accountingQueue, () -> {
            accountingQueue.close();
            onClose.run();
        });
    }

    private void setOperationsLogSink(int size, TimeValue expiration) {
        operationsLogSize = size;
        operationsLogExpiration = expiration;
        if (!isEnabled) {
            return;
        }
        updateOperationSink(size, expiration);
    }

    private void updateOperationSink(int size, TimeValue expiration) {
        LogSink<OperationContextLog> newSink = createSink(size, expiration, OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR,
            CrateCircuitBreakerService.OPERATIONS_LOG);
        LogSink<OperationContextLog> oldSink = operationsLogSink;
        newSink.addAll(oldSink);
        operationsLogSink = newSink;
        jobsLogs.updateOperationsLog(operationsLogSink);
        oldSink.close();
    }

    private void setStatsEnabled(boolean enableStats) {
        if (enableStats) {
            isEnabled = true;
            setOperationsLogSink(operationsLogSize, operationsLogExpiration);
            setJobsLogSink(jobsLogSize, jobsLogExpiration);
        } else {
            isEnabled = false;
            updateOperationSink(0, TimeValue.timeValueSeconds(0));
            updateJobSink(0, TimeValue.timeValueSeconds(0));
        }
    }

    /**
     * Indicates if statistics are gathered.
     * This result will change if the cluster settings is updated.
     */
    public boolean isEnabled() {
        return isEnabled;
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

    private JobsLogs statsTables() {
        return jobsLogs;
    }

    @Override
    public JobsLogs get() {
        return statsTables();
    }
}
