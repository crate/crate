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
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class QueryStatsTest {

    private final List<JobContextLog> log = ImmutableList.of(
        new JobContextLog(new JobContext(UUID.randomUUID(), "select name", 100L, null), null, 150L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "select name", 300L, null), null, 320L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "update t1 set x = 10", 400L, null), null, 420L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "insert into t1 (x) values (20)", 111L, null), null, 130L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "delete from t1", 410L, null), null, 415L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "delete from t1", 110L, null), null, 120L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "create table t1 (x int)", 105L, null), null, 106L)
    );

    @Test
    public void testCreateMetricsMap() throws Exception {
        Map<String, QueryStats.Metric> metricsByCommand = QueryStats.createMetricsMap(log, 2000, 0L);
        assertThat(metricsByCommand.size(), is(6));

        assertThat(metricsByCommand.get(QueryStats.Commands.SELECT).avgDurationInMs(), is(35.0));
        assertThat(metricsByCommand.get(QueryStats.Commands.SELECT).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(QueryStats.Commands.INSERT).avgDurationInMs(), is(19.0));
        assertThat(metricsByCommand.get(QueryStats.Commands.INSERT).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(QueryStats.Commands.UPDATE).avgDurationInMs(), is(20.0));
        assertThat(metricsByCommand.get(QueryStats.Commands.UPDATE).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(QueryStats.Commands.DELETE).avgDurationInMs(), is(7.0));
        assertThat(metricsByCommand.get(QueryStats.Commands.DELETE).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(QueryStats.Commands.UNCLASSIFIED).avgDurationInMs(), is(1.0));
        assertThat(metricsByCommand.get(QueryStats.Commands.UNCLASSIFIED).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(QueryStats.Commands.TOTAL).avgDurationInMs(), is(15.0));
        assertThat(metricsByCommand.get(QueryStats.Commands.TOTAL).statementsPerSec(), is(4.0));
    }

    @Test
    public void testDefaultValue() throws Exception {
        QueryStats queryStats = new QueryStats(new JobsLogs(() -> true));
        assertThat(queryStats.getSelectQueryFrequency(), is(0.0));
        assertThat(queryStats.getSelectQueryAverageDuration(), is(0.0));
    }
}
