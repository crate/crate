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
import io.crate.auth.user.User;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.planner.Plan.StatementType;
import io.crate.planner.operators.StatementClassifier.Classification;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class QueryStatsTest {

    private static final Classification SELECT_CLASSIFICATION = new Classification(StatementType.SELECT);
    private static final Classification UPDATE_CLASSIFICATION = new Classification(StatementType.UPDATE);
    private static final Classification DELETE_CLASSIFICATION = new Classification(StatementType.DELETE);
    private static final Classification INSERT_CLASSIFICATION = new Classification(StatementType.INSERT);
    private static final Classification DDL_CLASSIFICATION = new Classification(StatementType.DDL);

    private final List<JobContextLog> log = ImmutableList.of(
        new JobContextLog(new JobContext(UUID.randomUUID(), "select name", 100L, User.CRATE_USER, SELECT_CLASSIFICATION), null, 150L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "select name", 300L, User.CRATE_USER, SELECT_CLASSIFICATION), null, 320L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "update t1 set x = 10", 400L, User.CRATE_USER, UPDATE_CLASSIFICATION), null, 420L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "insert into t1 (x) values (20)", 111L, User.CRATE_USER, INSERT_CLASSIFICATION), null, 130L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "delete from t1", 410L, User.CRATE_USER, DELETE_CLASSIFICATION), null, 415L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "delete from t1", 110L, User.CRATE_USER, DELETE_CLASSIFICATION), null, 120L),
        new JobContextLog(new JobContext(UUID.randomUUID(), "create table t1 (x int)", 105L, User.CRATE_USER, DDL_CLASSIFICATION), null, 106L)
    );

    @Test
    public void testCreateMetricsMap() {
        Map<StatementType, QueryStats.Metric> metricsByCommand = QueryStats.createMetricsMap(log, 2000, 0L);
        assertThat(metricsByCommand.size(), is(6));

        assertThat(metricsByCommand.get(StatementType.SELECT).avgDurationInMs(), is(35.0));
        assertThat(metricsByCommand.get(StatementType.SELECT).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(StatementType.INSERT).avgDurationInMs(), is(19.0));
        assertThat(metricsByCommand.get(StatementType.INSERT).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.UPDATE).avgDurationInMs(), is(20.0));
        assertThat(metricsByCommand.get(StatementType.UPDATE).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.DELETE).avgDurationInMs(), is(7.0));
        assertThat(metricsByCommand.get(StatementType.DELETE).statementsPerSec(), is(1.0));

        assertThat(metricsByCommand.get(StatementType.UNDEFINED).avgDurationInMs(), is(1.0));
        assertThat(metricsByCommand.get(StatementType.UNDEFINED).statementsPerSec(), is(0.5));

        assertThat(metricsByCommand.get(StatementType.ALL).avgDurationInMs(), is(15.0));
        assertThat(metricsByCommand.get(StatementType.ALL).statementsPerSec(), is(4.0));
    }

    @Test
    public void testDefaultValue() {
        QueryStats queryStats = new QueryStats(new JobsLogs(() -> true));
        assertThat(queryStats.getSelectQueryFrequency(), is(0.0));
        assertThat(queryStats.getSelectQueryAverageDuration(), is(0.0));
    }
}
