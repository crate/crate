/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze.expressions;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.junit.Before;
import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class IntervalAnalysisTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (ts timestamp without time zone)")
            .build();
    }

    @Test
    public void test_psql_compact_format_from_string_with_start() {
        var symbol = e.asSymbol("INTERVAL '6 years 5 mons 4 days 03:02:01' YEAR");
        assertThat(symbol).isLiteral(new Period().withYears(6).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_psql_compact_format_from_string_with_start_end() {
        var symbol = e.asSymbol("INTERVAL '6 years 5 mons 4 days 03:02:01' YEAR TO MONTH");
        assertThat(symbol).isLiteral(new Period().withYears(6).withMonths(5)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_psql_compact_format_from_string_with_start_end1() {
        var symbol = e.asSymbol("INTERVAL '6 years 5 mons 4 days 03:02:01' DAY TO HOUR");
        assertThat(symbol).isLiteral(new Period().withYears(6).withMonths(5).withDays(4).withHours(3)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_interval() throws Exception {
        var symbol = e.asSymbol("INTERVAL '1' MONTH");
        assertThat(symbol).isLiteral(new Period().withMonths(1).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_negative_interval() throws Exception {
        var symbol = e.asSymbol("INTERVAL '-1' MONTH");
        assertThat(symbol).isLiteral(new Period().withMonths(-1).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_negative_negative_interval() throws Exception {
        var symbol = e.asSymbol("INTERVAL -'-1' MONTH");
        assertThat(symbol).isLiteral(new Period().withMonths(1).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_interval_conversion() throws Exception {
        var symbol = e.asSymbol("INTERVAL '1' HOUR to SECOND");
        assertThat(symbol).isLiteral(new Period().withSeconds(1).withPeriodType(PeriodType.yearMonthDayTime()));

        symbol = e.asSymbol("INTERVAL '100' DAY TO SECOND");
        assertThat(symbol).isLiteral(new Period().withMinutes(1).withSeconds(40)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_seconds_millis() throws Exception {
        var symbol = e.asSymbol("INTERVAL '1'");
        assertThat(symbol).isLiteral(new Period().withSeconds(1).withPeriodType(PeriodType.yearMonthDayTime()));

        symbol = e.asSymbol("INTERVAL '1.1'");
        assertThat(symbol).isLiteral(new Period().withSeconds(1).withMillis(100)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));

        symbol = e.asSymbol("INTERVAL '60.1'");
        assertThat(symbol).isLiteral(new Period().withMinutes(1).withMillis(100)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void testIntervalInvalidStartEnd() throws Exception {
        assertThatThrownBy(() -> e.asSymbol("INTERVAL '1' MONTH TO YEAR"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Startfield must be less significant than Endfield");
    }

    @Test
    public void test_odd() throws Exception {
        var symbol = e.asSymbol("INTERVAL '100.123' SECOND");
        assertThat(symbol).isLiteral(new Period().withMinutes(1).withSeconds(40).withMillis(123)
                                         .withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_more_odds() throws Exception {
        assertThatThrownBy(() -> e.asSymbol("INTERVAL '1-2 3 4-5-6'"))
            .isExactlyInstanceOf(ConversionException.class);
    }
}
