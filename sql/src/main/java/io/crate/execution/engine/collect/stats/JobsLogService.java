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
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.JobContextLogSizeEstimator;
import io.crate.breaker.OperationContextLogSizeEstimator;
import io.crate.breaker.SizeEstimator;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.data.Input;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.job.ContextLog;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.expression.reference.sys.operation.OperationContextLog;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.settings.CrateSetting;
import io.crate.sql.parser.SqlParser;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.function.Function;

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

    private static final FilterValidator FILTER_VALIDATOR = new FilterValidator();
    public static final CrateSetting<String> STATS_JOBS_LOG_FILTER = CrateSetting.of(
        new Setting<>(
            "stats.jobs_log_filter",
            "true",
            Function.identity(),
            FILTER_VALIDATOR,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic),
        DataTypes.STRING
    );
    public static final CrateSetting<String> STATS_JOBS_LOG_PERSIST_FILTER = CrateSetting.of(
        new Setting<>(
            "stats.jobs_log_persistent_filter",
            "false",
            Function.identity(),
            FILTER_VALIDATOR,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic),
        DataTypes.STRING
    );
    public static final CrateSetting<Integer> STATS_OPERATIONS_LOG_SIZE_SETTING = CrateSetting.of(Setting.intSetting(
        "stats.operations_log_size", 10_000, 0, Setting.Property.NodeScope, Setting.Property.Dynamic), DataTypes.INTEGER);
    public static final CrateSetting<TimeValue> STATS_OPERATIONS_LOG_EXPIRATION_SETTING = CrateSetting.of(Setting.timeSetting(
        "stats.operations_log_expiration", TimeValue.timeValueSeconds(0L), Setting.Property.NodeScope, Setting.Property.Dynamic),
        DataTypes.STRING);

    private static final JobContextLogSizeEstimator JOB_CONTEXT_LOG_ESTIMATOR = new JobContextLogSizeEstimator();
    private static final OperationContextLogSizeEstimator OPERATION_CONTEXT_LOG_SIZE_ESTIMATOR = new OperationContextLogSizeEstimator();

    private final ScheduledExecutorService scheduler;
    private final CrateCircuitBreakerService breakerService;
    private final InputFactory inputFactory;
    private final StaticTableReferenceResolver<JobContextLog> refResolver;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final EvaluatingNormalizer normalizer;
    private final TransactionContext transactionContext;

    private JobsLogs jobsLogs;
    LogSink<JobContextLog> jobsLogSink = NoopLogSink.instance();
    LogSink<OperationContextLog> operationsLogSink = NoopLogSink.instance();

    private ExpressionsInput<JobContextLog, Boolean> memoryFilter;
    private ExpressionsInput<JobContextLog, Boolean> persistFilter;

    private volatile boolean isEnabled;
    volatile int jobsLogSize;
    volatile TimeValue jobsLogExpiration;
    volatile int operationsLogSize;
    volatile TimeValue operationsLogExpiration;

    @Inject
    public JobsLogService(Settings settings,
                          ClusterSettings clusterSettings,
                          Functions functions,
                          ThreadPool threadPool,
                          CrateCircuitBreakerService breakerService) {
        this(settings, clusterSettings, functions, threadPool.scheduler(), breakerService);
    }

    @VisibleForTesting
    JobsLogService(Settings settings,
                   ClusterSettings clusterSettings,
                   Functions functions,
                   ScheduledExecutorService scheduledExecutorService,
                   CrateCircuitBreakerService breakerService) {
        super(settings);
        scheduler = scheduledExecutorService;
        this.breakerService = breakerService;
        this.inputFactory = new InputFactory(functions);
        this.refResolver = new StaticTableReferenceResolver<>(SysJobsLogTableInfo.expressions());
        TableRelation sysJobsLogRelation = new TableRelation(SysJobsLogTableInfo.INSTANCE);
        transactionContext = TransactionContext.systemTransactionContext();
        this.expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            transactionContext,
            ParamTypeHints.EMPTY,
            new NameFieldProvider(sysJobsLogRelation),
            null,
            Operation.READ
        );
        normalizer = new EvaluatingNormalizer(functions, RowGranularity.DOC, refResolver, sysJobsLogRelation);
        FILTER_VALIDATOR.validate = this::asSymbol;

        isEnabled = STATS_ENABLED_SETTING.setting().get(settings);
        jobsLogs = new JobsLogs(this::isEnabled);
        memoryFilter = createFilter(
            STATS_JOBS_LOG_FILTER.setting().get(settings), STATS_JOBS_LOG_FILTER.getKey());
        persistFilter = createFilter(
            STATS_JOBS_LOG_PERSIST_FILTER.setting().get(settings), STATS_JOBS_LOG_PERSIST_FILTER.getKey());

        setJobsLogSink(
            STATS_JOBS_LOG_SIZE_SETTING.setting().get(settings),
            STATS_JOBS_LOG_EXPIRATION_SETTING.setting().get(settings)
        );
        setOperationsLogSink(
            STATS_OPERATIONS_LOG_SIZE_SETTING.setting().get(settings), STATS_OPERATIONS_LOG_EXPIRATION_SETTING.setting().get(settings));

        clusterSettings.addSettingsUpdateConsumer(STATS_JOBS_LOG_FILTER.setting(), filter -> {
            JobsLogService.this.memoryFilter = createFilter(filter, STATS_JOBS_LOG_FILTER.getKey());
            updateJobSink(jobsLogSize, jobsLogExpiration);
        });
        clusterSettings.addSettingsUpdateConsumer(STATS_JOBS_LOG_PERSIST_FILTER.setting(), filter -> {
            JobsLogService.this.persistFilter = createFilter(filter, STATS_JOBS_LOG_PERSIST_FILTER.getKey());
            updateJobSink(jobsLogSize, jobsLogExpiration);
        });
        clusterSettings.addSettingsUpdateConsumer(STATS_ENABLED_SETTING.setting(), this::setStatsEnabled);
        clusterSettings.addSettingsUpdateConsumer(
            STATS_JOBS_LOG_SIZE_SETTING.setting(),
            STATS_JOBS_LOG_EXPIRATION_SETTING.setting(),
            this::setJobsLogSink);
        clusterSettings.addSettingsUpdateConsumer(
            STATS_OPERATIONS_LOG_SIZE_SETTING.setting(), STATS_OPERATIONS_LOG_EXPIRATION_SETTING.setting(), this::setOperationsLogSink);
    }

    private Symbol asSymbol(String expression) {
        try {
            return normalizer.normalize(
                expressionAnalyzer.convert(SqlParser.createExpression(expression), new ExpressionAnalysisContext()),
                transactionContext
            );
        } catch (Throwable t) {
            throw new IllegalArgumentException("Invalid filter expression: " + expression + ": " + t.getMessage(), t);
        }
    }

    private ExpressionsInput<JobContextLog, Boolean> createFilter(String filterExpression, String settingName) {
        Symbol filter = asSymbol(filterExpression);
        if (!filter.valueType().equals(DataTypes.BOOLEAN)) {
            throw new IllegalArgumentException(
                "Filter expression for " + settingName + " must result in a boolean, not: " + filter.valueType());
        }
        InputFactory.Context<NestableCollectExpression<JobContextLog, ?>> ctx = inputFactory.ctxForRefs(refResolver);
        @SuppressWarnings("unchecked")
        Input<Boolean> filterInput = (Input<Boolean>) ctx.add(filter);
        return new ExpressionsInput<>(filterInput, ctx.expressions());
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
        LogSink<JobContextLog> sink = createSink(
            size, expiration, JOB_CONTEXT_LOG_ESTIMATOR, CrateCircuitBreakerService.JOBS_LOG);
        LogSink<JobContextLog> newSink = sink.equals(NoopLogSink.instance()) ? sink : new FilteredLogSink<>(
            memoryFilter,
            persistFilter,
            jobContextLog -> new ParameterizedMessage(
                "Statement execution: stmt=\"{}\" duration={}, user={} error=\"{}\"",
                jobContextLog.statement(),
                jobContextLog.ended() - jobContextLog.started(),
                jobContextLog.username(),
                jobContextLog.errorMessage()
            ),
            sink
        );
        LogSink<JobContextLog> oldSink = jobsLogSink;
        newSink.addAll(oldSink);
        jobsLogSink = newSink;
        jobsLogs.updateJobsLog(newSink);
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
            long delay = 0L;
            ScheduledFuture<?> scheduledFuture = TimeBasedQEviction.scheduleTruncate(
                delay,
                clearInterval(expiration),
                q,
                scheduler,
                expiration
            );
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
            jobsLogs.resetMetricHistograms();
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

    @VisibleForTesting
    public int jobsLogSize() {
        return jobsLogSize;
    }


    private static class FilterValidator implements Setting.Validator<String> {

        /**
         * This is lazy initialized due to a bootstrapping problem:
         *
         * Settings need to be available in the SQLPlugin which is created *before* components like {@link Functions}.
         * But {@link Functions} is required to do the validation.
         */
        Consumer<String> validate = s -> { };

        @Override
        public void validate(String value, Map<Setting<String>, String> settings) {
            validate.accept(value);
        }
    }
}
