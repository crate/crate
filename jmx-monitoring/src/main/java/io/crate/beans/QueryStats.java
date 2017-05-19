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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.reference.sys.job.JobContextLog;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryStats implements QueryStatsMBean {

    static class Commands {
        static final String TOTAL = "total";
        static final String UNCLASSIFIED = "unclassified";

        static final String SELECT = "select";
        static final String INSERT = "insert";
        static final String UPDATE = "update";
        static final String DELETE = "delete";
    }

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
    private static final Pattern COMMAND_PATTERN = Pattern.compile("^\\s*(select|insert|update|delete).*");
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

    private final Supplier<Map<String, Metric>> metricByCommand;

    private volatile long lastUpdateTsInMillis = System.currentTimeMillis();

    public QueryStats(JobsLogs jobsLogs) {
        metricByCommand = Suppliers.memoizeWithExpiration(
            () -> {
                long currentTs = System.currentTimeMillis();
                Map<String, Metric> metricByCommand = createMetricsMap(jobsLogs.jobsLog(), currentTs, lastUpdateTsInMillis);
                lastUpdateTsInMillis = currentTs;
                return metricByCommand;
            },
            1,
            TimeUnit.SECONDS
        );
    }

    static Map<String, Metric> createMetricsMap(Iterable<JobContextLog> logEntries, long currentTs, long lastUpdateTs) {
        Map<String, Metric> metricsByCommand = new HashMap<>();
        long elapsedSinceLastUpdateInMs = currentTs - lastUpdateTs;

        Metric total = new Metric(0, elapsedSinceLastUpdateInMs);
        for (JobContextLog logEntry : logEntries) {
            if (logEntry.started() < lastUpdateTs || logEntry.started() > currentTs) {
                continue;
            }
            String command = getCommand(logEntry.statement());
            long duration = logEntry.ended() - logEntry.started();
            total.inc(duration);
            metricsByCommand.compute(command, (key, oldMetric) -> {
                if (oldMetric == null) {
                    return new Metric(duration, elapsedSinceLastUpdateInMs);
                }
                oldMetric.inc(duration);
                return oldMetric;
            });
        }
        metricsByCommand.put(Commands.TOTAL, total);
        return metricsByCommand;
    }

    private static String getCommand(String statement) {
        Matcher matcher = COMMAND_PATTERN.matcher(statement.toLowerCase(Locale.ENGLISH));
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return Commands.UNCLASSIFIED;
        }
    }

    @Override
    public double getSelectQueryFrequency() {
        return metricByCommand.get().getOrDefault(Commands.SELECT, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getInsertQueryFrequency() {
        return metricByCommand.get().getOrDefault(Commands.INSERT, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getUpdateQueryFrequency() {
        return metricByCommand.get().getOrDefault(Commands.UPDATE, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getDeleteQueryFrequency() {
        return metricByCommand.get().getOrDefault(Commands.DELETE, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getSelectQueryAverageDuration() {
        return metricByCommand.get().getOrDefault(Commands.SELECT, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getInsertQueryAverageDuration() {
        return metricByCommand.get().getOrDefault(Commands.INSERT, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getUpdateQueryAverageDuration() {
        return metricByCommand.get().getOrDefault(Commands.UPDATE, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getDeleteQueryAverageDuration() {
        return metricByCommand.get().getOrDefault(Commands.DELETE, DEFAULT_METRIC).avgDurationInMs();
    }

    @Override
    public double getOverallQueryFrequency() {
        return metricByCommand.get().getOrDefault(Commands.TOTAL, DEFAULT_METRIC).statementsPerSec();
    }

    @Override
    public double getOverallQueryAverageDuration() {
        return metricByCommand.get().getOrDefault(Commands.TOTAL, DEFAULT_METRIC).avgDurationInMs();
    }
}
