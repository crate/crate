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

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.crate.action.sql.SQLOperations.Session.UNNAMED;
import static io.crate.beans.QueryStats.MetricType.AVERAGE_DURATION;
import static io.crate.beans.QueryStats.MetricType.FREQUENCY;

public class QueryStats implements QueryStatsMBean {

    private final ESLogger logger;

    enum MetricType {
        FREQUENCY, AVERAGE_DURATION;
    }

    public static final String NAME = "io.crate.monitoring:type=QueryStats";

    private static final String QUERY_PATTERN = "^\\s*(%s).*";
    private static final String STMT = "SELECT COUNT(*) / ((CURRENT_TIMESTAMP - ?) / 1000.0), " +
        "AVG(ended - started), REGEXP_MATCHES(LOWER(stmt), ?)[1] " +
        "FROM sys.jobs_log " +
        "WHERE started BETWEEN ? AND CURRENT_TIMESTAMP " +
        "  AND REGEXP_MATCHES(LOWER(stmt), ?)[1] IS NOT NULL " +
        "GROUP BY 3";

    private final ConcurrentMap<String, Double> metrics;
    private final SQLOperations.Session session;
    @VisibleForTesting
    final ConcurrentMap<String, Long> lastQueried;

    public QueryStats(SQLOperations sqlOperations, Settings settings) {
        logger = Loggers.getLogger(QueryStats.class, settings);
        session = sqlOperations.createSession("sys", Option.NONE, 10000);
        session.parse(NAME, STMT, Collections.emptyList());

        lastQueried = new ConcurrentHashMap<>();
        metrics = new ConcurrentHashMap<>();
    }

    @Override
    public double getSelectQueryFrequency() {
        return updateAndGetLastSingleMetricValue("select", FREQUENCY);
    }

    @Override
    public double getInsertQueryFrequency() {
        return updateAndGetLastSingleMetricValue("insert", FREQUENCY);
    }

    @Override
    public double getUpdateQueryFrequency() {
        return updateAndGetLastSingleMetricValue("update", FREQUENCY);
    }

    @Override
    public double getDeleteQueryFrequency() {
        return updateAndGetLastSingleMetricValue("delete", FREQUENCY);
    }

    @Override
    public double getSelectQueryAverageDuration() {
        return updateAndGetLastSingleMetricValue("select", AVERAGE_DURATION);
    }

    @Override
    public double getInsertQueryAverageDuration() {
        return updateAndGetLastSingleMetricValue("insert", AVERAGE_DURATION);
    }

    @Override
    public double getUpdateQueryAverageDuration() {
        return updateAndGetLastSingleMetricValue("update", AVERAGE_DURATION);
    }

    @Override
    public double getDeleteQueryAverageDuration() {
        return updateAndGetLastSingleMetricValue("delete", AVERAGE_DURATION);
    }

    @Override
    public double getOverallQueryFrequency() {
        return updateAndGetLastOverallMetricValue("select|insert|delete|update", FREQUENCY);
    }

    @Override
    public double getOverallQueryAverageDuration() {
        return updateAndGetLastOverallMetricValue("select|insert|delete|update", AVERAGE_DURATION);
    }

    private double updateAndGetLastOverallMetricValue(String query, MetricType type) {
        return updateAndLastGetMetricValue(query, type, true);
    }

    private double updateAndGetLastSingleMetricValue(String query, MetricType type) {
        return updateAndLastGetMetricValue(query, type, false);
    }

    private double updateAndLastGetMetricValue(String query, MetricType type, boolean overall) {
        try {
            String queryUID = query + type;
            String queryPattern = String.format(Locale.ENGLISH, QUERY_PATTERN, query);
            long lastTs = updateAndGetLastExecutedTsFor(queryUID);

            session.bind(UNNAMED, NAME, Arrays.asList(lastTs, queryPattern, lastTs, queryPattern), null);
            session.execute(UNNAMED, 0, new ResultReceiver() {

                private final CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();
                private final List<Row> rows = new ArrayList<>();

                @Override
                public void setNextRow(Row row) {
                    rows.add(row);
                }

                @Override
                public void allFinished(boolean interrupted) {
                    double value = overall ?
                        getTotalMetricValue(rows, type.ordinal()) :
                        getMetricValue(rows, query, type.ordinal());
                    metrics.put(queryUID, value);
                    completionFuture.complete(interrupted);
                }

                @Override
                public void fail(@Nonnull Throwable t) {
                    logger.error("Failed to process metric results!", t);
                    completionFuture.completeExceptionally(t);
                }

                @Override
                public CompletableFuture<?> completionFuture() {
                    return completionFuture;
                }

                @Override
                public void batchFinished() {
                }
            });
            session.sync();
            return metrics.getOrDefault(queryUID, .0);
        } catch (Throwable t) {
            throw SQLExceptions.createSQLActionException(t);
        }
    }

    @VisibleForTesting
    double getMetricValue(List<Row> rows, String query, int metricIdx) {
        return rows.stream()
            .filter(row -> query.equalsIgnoreCase(BytesRefs.toString(row.get(2))))
            .mapToDouble(row -> (double) row.get(metricIdx))
            .findAny().orElse(.0);
    }

    @VisibleForTesting
    double getTotalMetricValue(List<Row> rows, int metricIdx) {
        return rows.stream()
            .mapToDouble(row -> (double) row.get(metricIdx))
            .sum();
    }

    @VisibleForTesting
    long updateAndGetLastExecutedTsFor(String queryUID) {
        long currentTs = System.currentTimeMillis();
        long lastTs = lastQueried.getOrDefault(queryUID, currentTs);

        lastQueried.put(queryUID, currentTs);
        return lastTs;
    }
}
