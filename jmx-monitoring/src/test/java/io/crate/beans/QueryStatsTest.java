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
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.action.sql.SQLOperations.Session.UNNAMED;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;

public class QueryStatsTest {

    private static final List<Row> ROWS = ImmutableList.of(
        new RowN(new Object[]{1.0, 1.1, new BytesRef("select")}),
        new RowN(new Object[]{2.0, 2.1, new BytesRef("update")}),
        new RowN(new Object[]{3.0, 3.1, new BytesRef("insert")}),
        new RowN(new Object[]{4.0, 4.1, new BytesRef("delete")})
    );

    private static QueryStats queryStats;
    private static SQLOperations.Session session = mock(SQLOperations.Session.class);

    @BeforeClass
    public static void beforeClass() {
        SQLOperations sqlOperations = mock(SQLOperations.class);

        when(sqlOperations.createSession(anyString(), anyObject(), anyObject(), anyInt())).thenReturn(session);
        doNothing().when(session).parse(anyString(), anyString(), anyListOf(DataType.class));

        queryStats = spy(new QueryStats(sqlOperations, Settings.EMPTY));
    }

    @Test
    public void testUpdateAndGetLastQueryExecutionTimestamp() throws InterruptedException {
        // If a query for a metric was executed for the first time, then the last executed ts will
        // be updated with the current timestamp and this value will be returned.
        long firstAvg = queryStats.updateAndGetLastExecutedTsFor("select" + QueryStats.MetricType.AVERAGE_DURATION);
        long firstQps = queryStats.updateAndGetLastExecutedTsFor("select" + QueryStats.MetricType.FREQUENCY);

        // The second call for the same metrics must return the last executed ts
        // (assigned to firstQps, firstAvg) and update them with a current timestamp.
        long lastAvg = queryStats.updateAndGetLastExecutedTsFor("select" + QueryStats.MetricType.AVERAGE_DURATION);
        long lastQps = queryStats.updateAndGetLastExecutedTsFor("select" + QueryStats.MetricType.FREQUENCY);
        assertThat(firstQps, is(lastQps));
        assertThat(firstAvg, is(lastAvg));

        // All the further invocation of the updateAndGetLastExecutedTsFor must return later ts.
        assertThat(lastAvg,
            lessThanOrEqualTo(queryStats.updateAndGetLastExecutedTsFor("select" + QueryStats.MetricType.AVERAGE_DURATION)));
        assertThat(lastQps,
            lessThanOrEqualTo(queryStats.updateAndGetLastExecutedTsFor("select" + QueryStats.MetricType.FREQUENCY)));
    }

    @Test
    public void testFrequencyMetricExtractedCorrectlyFromResponse() {
        assertThat(queryStats.getMetricValue(ROWS, "select", QueryStats.MetricType.FREQUENCY.ordinal()), is(1.0));
        assertThat(queryStats.getMetricValue(ROWS, "update", QueryStats.MetricType.FREQUENCY.ordinal()), is(2.0));
        assertThat(queryStats.getMetricValue(ROWS, "insert", QueryStats.MetricType.FREQUENCY.ordinal()), is(3.0));
        assertThat(queryStats.getMetricValue(ROWS, "delete", QueryStats.MetricType.FREQUENCY.ordinal()), is(4.0));
        assertThat(queryStats.getTotalMetricValue(ROWS, QueryStats.MetricType.FREQUENCY.ordinal()), is(10.0));
    }

    @Test
    public void testAverageDurationMetricExtractedCorrectlyFromResponse() {
        assertThat(queryStats.getMetricValue(ROWS, "select", QueryStats.MetricType.AVERAGE_DURATION.ordinal()), is(1.1));
        assertThat(queryStats.getMetricValue(ROWS, "update", QueryStats.MetricType.AVERAGE_DURATION.ordinal()), is(2.1));
        assertThat(queryStats.getMetricValue(ROWS, "insert", QueryStats.MetricType.AVERAGE_DURATION.ordinal()), is(3.1));
        assertThat(queryStats.getMetricValue(ROWS, "delete", QueryStats.MetricType.AVERAGE_DURATION.ordinal()), is(4.1));
        assertThat(queryStats.getTotalMetricValue(ROWS, QueryStats.MetricType.AVERAGE_DURATION.ordinal()), is(10.4));
    }

    @Test
    public void testGetMetricResultInCorrectSessionCalls() {
        queryStats.getDeleteQueryFrequency();
        String queryUID = "delete" + QueryStats.MetricType.FREQUENCY;
        verify(session, times(1)).bind(
            eq(UNNAMED),
            eq(QueryStats.NAME),
            eq(Arrays.asList(
                queryStats.lastQueried.get(queryUID), "^\\s*(delete).*",
                queryStats.lastQueried.get(queryUID), "^\\s*(delete).*"
            )),
            eq(null)
        );
        verify(session, times(1)).execute(eq(""), eq(0), any(BaseResultReceiver.class));
        verify(session, times(1)).sync();
    }
}
