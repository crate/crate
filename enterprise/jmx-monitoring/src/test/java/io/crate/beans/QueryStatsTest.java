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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.metadata.sys.ClassifiedMetrics.Metrics;
import io.crate.planner.Plan.StatementType;
import io.crate.planner.operators.StatementClassifier.Classification;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.crate.beans.QueryStats.createMetricsMap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class QueryStatsTest {

    private static final int HIGHEST_TRACKABLE_VALUE = 600000;

    private static final Classification SELECT_CLASSIFICATION = new Classification(StatementType.SELECT);
    private static final Classification UPDATE_CLASSIFICATION = new Classification(StatementType.UPDATE);
    private static final Classification DELETE_CLASSIFICATION = new Classification(StatementType.DELETE);
    private static final Classification INSERT_CLASSIFICATION = new Classification(StatementType.INSERT);
    private static final Classification DDL_CLASSIFICATION = new Classification(StatementType.DDL);

    private final List<Metrics> metrics = ImmutableList.of(
        createMetric(SELECT_CLASSIFICATION, 35),
        createMetric(SELECT_CLASSIFICATION, 35),
        createMetric(UPDATE_CLASSIFICATION, 20),
        createMetric(INSERT_CLASSIFICATION, 19),
        createMetric(DELETE_CLASSIFICATION, 5),
        createMetric(DELETE_CLASSIFICATION, 10),
        createMetric(DDL_CLASSIFICATION, 1)
    );

    private Metrics createMetric(Classification classification, long duration) {
        Metrics metrics = new Metrics(classification, HIGHEST_TRACKABLE_VALUE, 3);
        metrics.recordValue(duration);
        return metrics;
    }

    @Test
    public void testCreateMetricsMap() {
        Map<StatementType, QueryStats.Metric> metricsByCommand = createMetricsMap(metrics, Collections.emptyMap(), 2000, 0);
        assertThat(metricsByCommand.size(), is(6));

        assertThat(metricsByCommand.get(StatementType.SELECT).avgDurationInMs(), is(35.0));
        assertThat(metricsByCommand.get(StatementType.SELECT).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(StatementType.INSERT).avgDurationInMs(), is(19.0));
        assertThat(metricsByCommand.get(StatementType.INSERT).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.UPDATE).avgDurationInMs(), is(20.0));
        assertThat(metricsByCommand.get(StatementType.UPDATE).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.DELETE).avgDurationInMs(), is(7.5));
        assertThat(metricsByCommand.get(StatementType.DELETE).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(StatementType.UNDEFINED).avgDurationInMs(), is(1.0));
        assertThat(metricsByCommand.get(StatementType.UNDEFINED).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.ALL).avgDurationInMs(), is(17.857142857142858));
        assertThat(metricsByCommand.get(StatementType.ALL).statementsPerSec(), is(3.5));
    }

    @Test
    public void testCreateMetricsMapWithPreviouslyReadMetrics() {
        // setting up so that we've read a few of the configured metrics in a previous reading
        // the newly created reading should factor in the already read metrics and offset them from the measurements
        // ie. the new measurement will contain only one SELECT statement that was executed in the 2000 timeframe.
        Map<StatementType, QueryStats.Metric> previouslyReadMetrics = ImmutableMap.of(
            StatementType.SELECT, new QueryStats.Metric(null, 35, 1, 2000),
            StatementType.DELETE, new QueryStats.Metric(null, 5, 1, 2000),
            StatementType.UPDATE, new QueryStats.Metric(null, 20, 1, 2000),
            StatementType.ALL, new QueryStats.Metric(null, 60, 3, 2000)
        );

        Map<StatementType, QueryStats.Metric> metricsByCommand = createMetricsMap(metrics, previouslyReadMetrics, 2000, 0);
        assertThat(metricsByCommand.size(), is(6));

        assertThat(metricsByCommand.get(StatementType.SELECT).avgDurationInMs(), is(35.0));
        assertThat(metricsByCommand.get(StatementType.SELECT).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.INSERT).avgDurationInMs(), is(19.0));
        assertThat(metricsByCommand.get(StatementType.INSERT).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.UPDATE).avgDurationInMs(), is(0.0));
        assertThat(metricsByCommand.get(StatementType.UPDATE).statementsPerSec(), is(0.0));

        assertThat(metricsByCommand.get(StatementType.DELETE).avgDurationInMs(), is(10.0));
        assertThat(metricsByCommand.get(StatementType.DELETE).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.UNDEFINED).avgDurationInMs(), is(1.0));
        assertThat(metricsByCommand.get(StatementType.UNDEFINED).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.ALL).avgDurationInMs(), is(16.25));
        assertThat(metricsByCommand.get(StatementType.ALL).statementsPerSec(), is(2.0));
    }

    @Test
    public void testSameTypeWithDifferentLabelsClassificationsAreMerged() {
        List<Metrics> selectMetrics = ImmutableList.of(
            createMetric(SELECT_CLASSIFICATION, 35),
            createMetric(new Classification(StatementType.SELECT, ImmutableSet.of("lookup")), 55)
        );

        Map<StatementType, QueryStats.Metric> metricsByCommand = createMetricsMap(selectMetrics, Collections.emptyMap(), 2000, 0);
        assertThat(metricsByCommand.size(), is(2));

        assertThat(metricsByCommand.get(StatementType.SELECT).avgDurationInMs(), is(45.0));
        assertThat(metricsByCommand.get(StatementType.SELECT).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(StatementType.ALL).avgDurationInMs(), is(45.0));
        assertThat(metricsByCommand.get(StatementType.ALL).statementsPerSec(), is(1.0));
    }

    @Test
    public void testDefaultValue() {
        QueryStats queryStats = new QueryStats(new JobsLogs(() -> true));
        assertThat(queryStats.getSelectQueryFrequency(), is(0.0));
        assertThat(queryStats.getSelectQueryAverageDuration(), is(0.0));
    }
}
