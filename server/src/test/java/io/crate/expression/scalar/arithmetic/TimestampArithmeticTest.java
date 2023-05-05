/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar.arithmetic;

import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class TimestampArithmeticTest extends ScalarTestCase {

    @Test
    public void test_timestamp_add() {
        assertEvaluate("'2022-12-05T11:22:33.123456789'::timestamp + '-0022-12-03T11:22:33.123456789'::timestamp",
                       -61162132493754L);
        assertEvaluate("'2022-12-03T11:22:33.123456789'::timestamp + '-2022-12-05T11:22:33.123456789'::timestamp",
                       -124276036493754L);
    }

    @Test
    public void test_timestamp_subtract() {
        assertEvaluate("'2000-03-21T23:33:44.999999999'::timestamp - '2022-12-05T11:22:33.123456789'::timestamp",
                       new Period(-22, -8, 0, -13, -11, -48, -48, -124,
                                  PeriodType.yearMonthDayTime()));
        assertEvaluate("'2022-12-05T11:22:33.123456789'::timestamp - '-2000-03-21T23:33:44.999999999'::timestamp",
                       new Period(4022, 8, 0, 13, 11, 48, 48, 124,
                                  PeriodType.yearMonthDayTime()));
        assertEvaluate("'2022-12-05T11:22:33.123456789+05:30'::timestamptz - '2022-12-03T11:22:33.123456789-02:15'::timestamptz",
                       new Period(0, 0, 0, 1, 16, 15, 0, 0,
                                  PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_timestamp_subtract_with_nulls() {
        assertEvaluateNull("'2000-03-21T23:33:44.999999999'::timestamp - null::timestamp");
        assertEvaluateNull("null::timestamp - '-2000-03-21T23:33:44.999999999'::timestamp");
        assertEvaluateNull("null::timestamp - null::timestamptz");
    }
}
