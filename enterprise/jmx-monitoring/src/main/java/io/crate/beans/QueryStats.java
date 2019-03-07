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
import io.crate.metadata.sys.MetricsView;
import io.crate.planner.Plan.StatementType;
import io.crate.planner.operators.StatementClassifier;

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

        private long failedCount;
        private long totalCount;
        private long sumOfDurations;

        Metric(long sumOfDurations, long totalCount, long failedCount) {
            this.sumOfDurations = sumOfDurations;
            this.totalCount = totalCount;
            this.failedCount = failedCount;
        }

        void inc(long duration, long totalCount, long failedCount) {
            this.sumOfDurations += duration;
            this.totalCount += totalCount;
            this.failedCount += failedCount;
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
    private static final Metric DEFAULT_METRIC = new Metric(0, 0, 0) {

        @Override
        void inc(long duration, long totalCount, long failedCount) {
            throw new AssertionError("inc must not be called on default metric - it's immutable");
        }
    };
    private final Supplier<Map<StatementType, Metric>> metricByStmtType;

    public QueryStats(JobsLogs jobsLogs) {
        metricByStmtType = Suppliers.memoizeWithExpiration(
            () -> createMetricsMap(jobsLogs.metrics()),
            1,
            TimeUnit.SECONDS
        );
    }

    static Map<StatementType, Metric> createMetricsMap(Iterable<MetricsView> metrics) {
        Map<StatementType, Metric> metricsByStmtType = new HashMap<>();
        for (MetricsView classifiedMetrics : metrics) {
            long sumOfDurations = classifiedMetrics.sumOfDurations();
            long failedCount = classifiedMetrics.failedCount();
            metricsByStmtType.compute(classificationType(classifiedMetrics.classification()), (key, oldMetric) -> {
                if (oldMetric == null) {
                    return new Metric(sumOfDurations, classifiedMetrics.totalCount(), failedCount);
                }
                oldMetric.inc(sumOfDurations, classifiedMetrics.totalCount(), failedCount);
                return oldMetric;
            });
        }
        return metricsByStmtType;
    }

    private static StatementType classificationType(StatementClassifier.Classification classification) {
        if (classification == null || !CLASSIFIED_STATEMENT_TYPES.contains(classification.type())) {
            return StatementType.UNDEFINED;
        }
        return classification.type();
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

    @Override
    public long getSelectQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.SELECT, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getInsertQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.INSERT, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getUpdateQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.UPDATE, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getDeleteQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.DELETE, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getManagementQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.MANAGEMENT, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getDDLQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.DDL, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getCopyQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.COPY, DEFAULT_METRIC).failedCount();
    }

    @Override
    public long getUndefinedQueryFailedCount() {
        return metricByStmtType.get().getOrDefault(StatementType.UNDEFINED, DEFAULT_METRIC).failedCount();
    }
}
