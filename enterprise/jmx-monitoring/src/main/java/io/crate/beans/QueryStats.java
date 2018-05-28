/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.beans;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.planner.Plan.StatementType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class QueryStats implements QueryStatsMBean {

    private static final Set<StatementType> CLASSIFIED_STATEMENT_TYPES =
        ImmutableSet.of(StatementType.SELECT, StatementType.INSERT, StatementType.UPDATE, StatementType.DELETE);

    static class Metric {

        private final long elapsedSinceUpdateInMs;

        private long count;
        private long sumOfDurations;

        Metric(long duration, long elapsedSinceUpdateInMs) {
            this.elapsedSinceUpdateInMs = elapsedSinceUpdateInMs;
            sumOfDurations = duration;
            count = 1;
        }

        void inc(long duration) {
            sumOfDurations += duration;
            count++;
        }

        double statementsPerSec() {
            return count / (elapsedSinceUpdateInMs / 1000.0);
        }

        double avgDurationInMs() {
            return sumOfDurations / count;
        }
    }

    public static final String NAME = "io.crate.monitoring:type=QueryStats";
    private static final Metric DEFAULT_METRIC = new Metric(0, 0) {

        @Override
        void inc(long duration) {
            throw new AssertionError("inc must not be called on default metric - it's immutable");
        }

        @Override
        double statementsPerSec() {
            return 0.0;
        }

        @Override
        double avgDurationInMs() {
            return 0.0;
        }
    };

    private final Supplier<Map<StatementType, Metric>> metricByStmtType;

    private volatile long lastUpdateTsInMillis = System.currentTimeMillis();

    public QueryStats(JobsLogs jobsLogs) {
        metricByStmtType = Suppliers.memoizeWithExpiration(
            () -> {
                long currentTs = System.currentTimeMillis();
                Map<StatementType, Metric> metricByCommand = createMetricsMap(jobsLogs.jobsLog(), currentTs, lastUpdateTsInMillis);
                lastUpdateTsInMillis = currentTs;
                return metricByCommand;
            },
            1,
            TimeUnit.SECONDS
        );
    }

    static Map<StatementType, Metric> createMetricsMap(Iterable<JobContextLog> logEntries, long currentTs, long lastUpdateTs) {
        Map<StatementType, Metric> metricsByStmtType = new HashMap<>();
        long elapsedSinceLastUpdateInMs = currentTs - lastUpdateTs;

        Metric total = new Metric(0, elapsedSinceLastUpdateInMs);
        for (JobContextLog logEntry : logEntries) {
            if (logEntry.started() < lastUpdateTs || logEntry.started() > currentTs) {
                continue;
            }
            long duration = logEntry.ended() - logEntry.started();
            total.inc(duration);
            metricsByStmtType.compute(classificationType(logEntry), (key, oldMetric) -> {
                if (oldMetric == null) {
                    return new Metric(duration, elapsedSinceLastUpdateInMs);
                }
                oldMetric.inc(duration);
                return oldMetric;
            });
        }
        metricsByStmtType.put(StatementType.ALL, total);
        return metricsByStmtType;
    }

    private static StatementType classificationType(JobContextLog logEntry) {
        if (logEntry.classification() == null || !CLASSIFIED_STATEMENT_TYPES.contains(logEntry.classification().type())) {
            return StatementType.UNDEFINED;
        }
        return logEntry.classification().type();
    }

    @Override
    public double getSelectQueryFrequency() {
        return metricByStmtType.get().getOrDefault(StatementType.SELECT, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getInsertQueryFrequency() {
        return metricByStmtType.get().getOrDefault(StatementType.INSERT, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getUpdateQueryFrequency() {
        return metricByStmtType.get().getOrDefault(StatementType.UPDATE, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getDeleteQueryFrequency() {
        return metricByStmtType.get().getOrDefault(StatementType.DELETE, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getOverallQueryFrequency() {
        return metricByStmtType.get().getOrDefault(StatementType.ALL, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getSelectQueryAverageDuration() {
        return metricByStmtType.get().getOrDefault(StatementType.SELECT, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getInsertQueryAverageDuration() {
        return metricByStmtType.get().getOrDefault(StatementType.INSERT, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getUpdateQueryAverageDuration() {
        return metricByStmtType.get().getOrDefault(StatementType.UPDATE, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getDeleteQueryAverageDuration() {
        return metricByStmtType.get().getOrDefault(StatementType.DELETE, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getOverallQueryAverageDuration() {
        return metricByStmtType.get().getOrDefault(StatementType.ALL, DEFAULT_METRIC).avgDurationInMs();
    }
}
