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
import io.crate.metadata.sys.ClassifiedMetrics.Metrics;
import io.crate.planner.Plan.StatementType;
import io.crate.planner.operators.StatementClassifier;
import org.HdrHistogram.Histogram;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class QueryStats implements QueryStatsMBean {

    private static final Set<StatementType> CLASSIFIED_STATEMENT_TYPES =
        ImmutableSet.of(StatementType.SELECT, StatementType.INSERT, StatementType.UPDATE, StatementType.DELETE,
            StatementType.MANAGEMENT, StatementType.COPY, StatementType.DDL);

    static class Metric {

        private final Metric previousReading;
        private long elapsedSinceLastUpdateInMs;
        private long failedCount;
        private long totalCount;
        private long sumOfDurations;

        Metric(@Nullable Metric previousReading,
               long sumOfDurations,
               long totalCount,
               long failedCount,
               long elapsedSinceLastUpdateInMs) {
            this.elapsedSinceLastUpdateInMs = elapsedSinceLastUpdateInMs;
            this.sumOfDurations = sumOfDurations;
            this.totalCount = totalCount;
            this.failedCount = failedCount;
            this.previousReading = previousReading;
        }

        void inc(long duration, long totalCount, long failedCount) {
            this.sumOfDurations += duration;
            this.totalCount += totalCount;
            this.failedCount += failedCount;
        }

        double statementsPerSec() {
            if (previousReading == null) {
                return totalCount / (elapsedSinceLastUpdateInMs / 1000.0);
            } else {
                return (totalCount - previousReading.totalCount()) / (elapsedSinceLastUpdateInMs / 1000.0);
            }
        }

        double avgDurationInMs() {
            if (totalCount == 0) {
                return 0;
            }

            if (previousReading == null) {
                return sumOfDurations / (double) totalCount;
            } else {
                long countSinceLastRead = totalCount - previousReading.totalCount();
                if (countSinceLastRead == 0) {
                    return 0;
                }
                return (sumOfDurations - previousReading.sumOfDurations()) /
                       (double) countSinceLastRead;
            }
        }

        long totalCount() {
            return totalCount;
        }

        long sumOfDurations() {
            return sumOfDurations;
        }

        long failedCount() {
            return failedCount;
        }
    }

    public static final String NAME = "io.crate.monitoring:type=QueryStats";
    private static final Metric DEFAULT_METRIC = new Metric(null, 0, 0, 0, 0) {

        @Override
        void inc(long duration, long totalCount, long failedCount) {
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
    private Map<StatementType, Metric> previouslyReadMetrics = Collections.emptyMap();
    private final Supplier<Map<StatementType, Metric>> metricByStmtType;

    private volatile long lastUpdateTsInMillis = System.currentTimeMillis();

    public QueryStats(JobsLogs jobsLogs) {
        metricByStmtType = Suppliers.memoizeWithExpiration(
            () -> {
                long currentTs = System.currentTimeMillis();
                previouslyReadMetrics = createMetricsMap(jobsLogs.metrics(), previouslyReadMetrics, currentTs, lastUpdateTsInMillis);
                lastUpdateTsInMillis = currentTs;
                return previouslyReadMetrics;
            },
            1,
            TimeUnit.SECONDS
        );
    }

    static Map<StatementType, Metric> createMetricsMap(Iterable<Metrics> metrics,
                                                       Map<StatementType, Metric> previouslyReadMetrics,
                                                       long currentTs,
                                                       long lastUpdateTs) {
        Map<StatementType, Metric> metricsByStmtType = new HashMap<>();
        long elapsedSinceLastUpdateInMs = currentTs - lastUpdateTs;

        Metric total = new Metric(previouslyReadMetrics.get(StatementType.ALL), 0, 0, 0, elapsedSinceLastUpdateInMs);
        for (Metrics classifiedMetrics : metrics) {
            Histogram histogram = classifiedMetrics.histogram();
            long sumOfDurations = classifiedMetrics.sumOfDurations();
            long failedCount = classifiedMetrics.failedCount();

            total.inc(sumOfDurations, histogram.getTotalCount(), failedCount);
            metricsByStmtType.compute(classificationType(classifiedMetrics.classification()), (key, oldMetric) -> {
                if (oldMetric == null) {
                    return new Metric(previouslyReadMetrics.get(key), sumOfDurations, histogram.getTotalCount(), failedCount, elapsedSinceLastUpdateInMs);
                }
                oldMetric.inc(sumOfDurations, histogram.getTotalCount(), failedCount);
                return oldMetric;
            });
        }
        metricsByStmtType.put(StatementType.ALL, total);
        return metricsByStmtType;
    }

    private static StatementType classificationType(StatementClassifier.Classification classification) {
        if (classification == null || !CLASSIFIED_STATEMENT_TYPES.contains(classification.type())) {
            return StatementType.UNDEFINED;
        }
        return classification.type();
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

    @Override
    public long getSelectQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.SELECT, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getInsertQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.INSERT, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getUpdateQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.UPDATE, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getDeleteQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.DELETE, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getManagementQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.MANAGEMENT, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getDDLQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.DDL, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getCopyQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.COPY, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getUndefinedQueryTotalCount() {
        return metricByStmtType.get().getOrDefault(StatementType.UNDEFINED, DEFAULT_METRIC).totalCount();
    }

    @Override
    public long getSelectQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.SELECT, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getInsertQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.INSERT, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getUpdateQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.UPDATE, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getDeleteQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.DELETE, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getManagementQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.MANAGEMENT, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getDDLQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.DDL, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getCopyQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.COPY, DEFAULT_METRIC).sumOfDurations();
    }

    @Override
    public long getUndefinedQuerySumOfDurations() {
        return metricByStmtType.get().getOrDefault(StatementType.UNDEFINED, DEFAULT_METRIC).sumOfDurations();
    }
}
