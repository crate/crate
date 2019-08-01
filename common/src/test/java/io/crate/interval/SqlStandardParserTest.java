/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.interval;

import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class SqlStandardParserTest {

    @Test
    public void parse_year_month_day_hours_minutes() {
        Period result = SQLStandardIntervalParser.apply("120-1 1 15:30");
        Assert.assertEquals(new Period().withYears(120).withMonths(1).withDays(1).withHours(15).withMinutes(30), result);
    }

    @Test
    public void negative_parse_year_month_negative_day_negative_hours_minutes() {
        Period result = SQLStandardIntervalParser.apply("-120-1 -1 -15:30");
        Assert.assertEquals(new Period().withYears(-120).withMonths(-1).withDays(-1).withHours(-15).withMinutes(-30), result);
    }

    @Test
    public void parse_year_month_day() {
        Period result = SQLStandardIntervalParser.apply("120-1 1");
        Assert.assertEquals(new Period().withYears(120).withMonths(1).withSeconds(1), result);
    }

    @Test
    public void parse_negative_year_month_negative_day() {
        Period result = SQLStandardIntervalParser.apply("-120-1 -1");
        Assert.assertEquals(new Period().withYears(-120).withMonths(-1).withSeconds(-1), result);
    }

    @Test
    public void parse_year_month() {
        Period result = SQLStandardIntervalParser.apply("120-1");
        Assert.assertEquals(new Period().withYears(120).withMonths(1), result);
    }

    @Test
    public void parse_negative_year_month() {
        Period result = SQLStandardIntervalParser.apply("-120-1");
        Assert.assertEquals(new Period().withYears(-120).withMonths(-1), result);
    }

    @Test
    public void parse_year_month_hours_minutes() {
        Period result = SQLStandardIntervalParser.apply("120-1 15:30");
        Assert.assertEquals(new Period().withYears(120).withMonths(1).withHours(15).withMinutes(30), result);
    }

    @Test
    public void parse_hours_minutes() {
        Period result = SQLStandardIntervalParser.apply("15:30");
        Assert.assertEquals(new Period().withHours(15).withMinutes(30), result);
    }

    @Test
    public void parse_negative_hours_minutes() {
        Period result = SQLStandardIntervalParser.apply("-15:30");
        Assert.assertEquals(new Period().withHours(-15).withMinutes(-30), result);
    }

    @Test
    public void parse_hours_minutes_seconds() {
        Period result = SQLStandardIntervalParser.apply("15:30:10");
        Assert.assertEquals(new Period().withHours(15).withMinutes(30).withSeconds(10), result);
    }

    @Test
    public void parse_days_hours_minutes_seconds() {
        Period result = SQLStandardIntervalParser.apply("1 15:30:10");
        Assert.assertEquals(new Period().withDays(1).withHours(15).withMinutes(30).withSeconds(10), result);
    }

    @Test
    public void parse_negative_days_negative_hours_minutes_seconds() {
        Period result = SQLStandardIntervalParser.apply("-1 -15:30:10");
        Assert.assertEquals(new Period().withDays(-1).withHours(-15).withMinutes(-30).withSeconds(-10), result);
    }

    @Test
    public void parse_days_seconds() {
        Period result = SQLStandardIntervalParser.apply("1 1");
        Assert.assertEquals(new Period().withDays(1).withSeconds(1), result);
    }

    @Test
    public void parse_negtive_days_negative_seconds() {
        Period result = SQLStandardIntervalParser.apply("-1 -1");
        Assert.assertEquals(new Period().withDays(-1).withSeconds(-1), result);
    }

}
