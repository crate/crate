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

import io.crate.test.integration.CrateUnitTest;
import org.joda.time.Period;
import org.junit.Test;

import static org.hamcrest.core.Is.is;


public class PGIntervalParserTest extends CrateUnitTest {

    @Test
    public void test_psql_format_from_string() {
        Period period = PGIntervalParser.apply("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs");
        assertThat(period,
            is(new Period().withYears(1).withMonths(1).withDays(1).withHours(1).withMinutes(1).withSeconds(1)));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_ago() {
        Period period = PGIntervalParser.apply("@ 1 year 1 mon 1 day 1 hour 1 minute 1 secs ago");
        assertThat(period,
            is(new Period().withYears(-1).withMonths(-1).withDays(-1).withHours(-1).withMinutes(-1).withSeconds(-1)));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_negative_values() {
        Period period = PGIntervalParser.apply("@ 1 year -23 hours -3 mins -3.30 secs");
        assertThat(period,
            is(new Period().withYears(1).withHours(-23).withMinutes(-3).withSeconds(-3).withMillis(-300)));
    }

    @Test
    public void test_psql_verbose_format_from_string_with_negative_values_and_ago() {
        Period period = PGIntervalParser.apply("@ 1 year -23 hours -3 mins -3.30 secs ago");
        assertThat(period,
            is(new Period().withYears(-1).withHours(23).withMinutes(3).withSeconds(3).withMillis(300)));
    }

    @Test
    public void test_psql_compact_format_from_string() {
        Period period = PGIntervalParser.apply("6 years 5 mons 4 days 03:02:01");
        assertThat(period,
            is(new Period().withYears(6).withMonths(5).withDays(4).withHours(3).withMinutes(2).withSeconds(1)));
    }
    @Test
    public void test_weeks() {
        Period period = PGIntervalParser.apply("1 week");
        assertThat(period, is(new Period().withDays(7)));
    }
}
