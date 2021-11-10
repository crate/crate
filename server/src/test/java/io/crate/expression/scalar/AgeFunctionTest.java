package io.crate.expression.scalar;

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



import org.junit.Test;
import java.time.*;
import java.time.temporal.ChronoUnit;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.Matchers.*;

public class AgeFunctionTest extends ScalarTestCase
{

    @Test
    public void testAgeCanBeAddressed() throws Exception
    {
        assertEvaluate("age(1401777485000)", is(notNullValue()));
        assertEvaluate("age('2021-09-03'::timestamp without time zone)", is(notNullValue()));
        assertEvaluate("age('2021-09-03'::timestamp)",  is(notNullValue()));
        assertEvaluate("age(timestamp '2021-09-03',timestamp '2021-08-03')",  is(notNullValue()));
        assertEvaluate("pg_catalog.age(1401777485000)", is(notNullValue()));
        assertEvaluate("pg_catalog.age('2021-09-03'::timestamp without time zone)", is(notNullValue()));
        assertEvaluate("pg_catalog.age('2021-09-03'::timestamp)",  is(notNullValue()));
        assertEvaluate("pg_catalog.age(timestamp '2021-09-03',timestamp '2021-08-03')",  is(notNullValue()));

    }


    @Test
    public void testAgeDateTimeZoneAware() throws Exception {

        long midnightMoscow = LocalDate.now( ZoneOffset.UTC ).atTime(3,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        long midnightVienna = LocalDate.now( ZoneOffset.UTC ).atTime(1,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        long midnightUTC = LocalDate.now( ZoneOffset.UTC ).atStartOfDay( ZoneOffset.UTC ).toInstant().toEpochMilli();
        Long expected3 =midnightMoscow-LocalDate.of(2021, Month.NOVEMBER, 8).atTime(3,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected1 =midnightVienna-LocalDate.of(2021, Month.NOVEMBER, 8).atTime(1,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected0 =midnightUTC-LocalDate.of(2021, Month.NOVEMBER, 8).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("age('2021-11-08')", expected0);
        assertEvaluate("age('Europe/Vienna', '2021-11-08')", expected1);  // +0100
        assertEvaluate("age('+01:00', '2021-11-08')",expected1);         // +0100
        assertEvaluate("age('CET', '2021-11-08')", expected1);           // +0100
        assertEvaluate("age('Europe/Moscow', '2021-11-08')", expected3); // +0300
        assertEvaluate("age('+03:00', '2021-11-08')",expected3);         // +0300


    }
    @Test
    public void testAgeDateTimeTimeZoneAware() throws Exception {

        long midnightMoscow = LocalDate.now( ZoneOffset.UTC ).atTime(3,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        long midnightVienna = LocalDate.now( ZoneOffset.UTC ).atTime(1,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        long midnightUTC = LocalDate.now( ZoneOffset.UTC ).atStartOfDay( ZoneOffset.UTC ).toInstant().toEpochMilli();
        Long expected4 = midnightMoscow - LocalDate.of(2021, Month.NOVEMBER, 8).atTime(4,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected2 = midnightVienna - LocalDate.of(2021, Month.NOVEMBER, 8).atTime(2,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected1 = midnightUTC - LocalDate.of(2021, Month.NOVEMBER, 8).atTime(1,0).toEpochSecond(ZoneOffset.UTC) * 1000;
        assertEvaluate("age('2021-11-08 01:00')", expected1);
        assertEvaluate("age('Europe/Vienna', '2021-11-08 01:00')", expected2);  // +0100
        assertEvaluate("age('+01:00', '2021-11-08 01:00')",expected2);         // +0100
        assertEvaluate("age('CET', '2021-11-08 01:00')", expected2);           // +0100
        assertEvaluate("age('Europe/Moscow', '2021-11-08 01:00')", expected4); // +0300
        assertEvaluate("age('+03:00', '2021-11-08 01:00')",expected4);         // +0300

    }


    @Test
    public void testAgeWithTruncatedTimeToMidnight() throws Exception
    {
        long at5Nov = LocalDate.of(2021, Month.NOVEMBER, 5).atTime(3,43).toEpochSecond(ZoneOffset.UTC) * 1000;
        long midnight = LocalDate.now( ZoneOffset.UTC ).atStartOfDay( ZoneOffset.UTC ).toInstant().toEpochMilli();
        Long expected = midnight - at5Nov;

        assertEvaluate("pg_catalog.age(timestamp '2021-11-05 03:43')", expected);


    }


    @Test
    public void testAgeDiffWithYearsMonthsAndDays() throws Exception
    {
        long expected1 = LocalDate.of(2020, Month.SEPTEMBER, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        long expected2 = LocalDate.of(2021, Month.NOVEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected = expected2 - expected1;
        assertEvaluate("age( timestamp '2021-11-03', timestamp '2020-09-01')", expected);
        assertEvaluate("age( '2021-11-03'::timestamp without time zone, '2020-09-01'::timestamp without time zone)", expected);
        assertEvaluate("age( "+expected2+", "+expected1+")", expected);


    }

    @Test
    public void testAgeDiffWithYears() throws Exception
    {
        long expected1 = LocalDate.of(2020, Month.NOVEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        long expected2 = LocalDate.of(2021, Month.NOVEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected = expected2 - expected1;
        assertEvaluate("age( timestamp '2021-11-03', timestamp '2020-11-03')", expected);
        assertEvaluate("age( "+expected2+", "+expected1+")", expected);


    }

    @Test
    public void testAgeDiffWithMonths() throws Exception
    {
        long expected1 = LocalDate.of(2021, Month.SEPTEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        long expected2 = LocalDate.of(2021, Month.NOVEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected = expected2 - expected1;
        assertEvaluate("age( timestamp '2021-11-03', timestamp '2021-09-03')", expected);
        assertEvaluate("age( "+expected2+", "+expected1+")", expected);


    }

    @Test
    public void testAgeDiffWithDays() throws Exception
    {
        long expected1 = LocalDate.of(2021, Month.NOVEMBER, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        long expected2 = LocalDate.of(2021, Month.NOVEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected = expected2 - expected1;
        assertEvaluate("age( timestamp '2021-11-03', timestamp '2021-11-01')", expected);
        assertEvaluate("age( "+expected2+", "+expected1+")", expected);


    }

    @Test
    public void testAgeWithIdenticalDates() throws Exception
    {

        Long expected = 0l;
        assertEvaluate("age(timestamp '2021-09-01', timestamp '2021-09-01')", expected);


    }


    @Test
    public void testCallTwiceSameResult() throws Exception{

        assertEvaluate("age(timestamp '2021-09-01', timestamp '2021-08-01') = age(timestamp '2021-09-01', timestamp '2021-08-01')", true);

    }

    @Test
    public void testNegativeAge() throws Exception
    {
        long expected1 = LocalDate.of(2020, Month.SEPTEMBER, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        long expected2 = LocalDate.of(2021, Month.NOVEMBER, 3).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000;
        Long expected = expected1 - expected2;
        // ("age('2020-09-01', '2021-11-03') -> "-1 years -2 mons -2 days"
        assertEvaluate("age( timestamp '2020-09-01',timestamp '2021-11-03')", expected);
    }


}

