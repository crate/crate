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

package io.crate.expression.scalar.arithmetic;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;

public class IntervalFunctionTest extends ScalarTestCase {

    @Test
    public void test_interval_to_interval() {
        assertEvaluate("interval '1 second' + interval '1 second'",
                       Period.seconds(2).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '1100 years' + interval '2000 years'",
                       Period.years(3100).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '-10 years' + interval '1 years'",
                       Period.years(-9).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '2 second' - interval '1 second'",
                       Period.seconds(1).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '-1 second' - interval '-1 second'",
                       Period.seconds(0).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '1 month' + interval '1 year'",
                       Period.years(1).withMonths(1).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_multiply_by_integer() {
        assertEvaluate("2 * interval '2 years 1 month 10 days'",
                       Period.years(4).withMonths(2).withDays(20).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("0 * interval '10 second'",
                       Period.seconds(0).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("-10 * interval '1 day'",
                       Period.days(-10).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("2147483647 * interval '1 year'",
                       Period.years(2147483647).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("-2147483648 * interval '1 year'",
                       Period.years(-2147483648).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '2 years 1 month 10 days' * 2",
                       Period.years(4).withMonths(2).withDays(20).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '10 second' * 0",
                       Period.seconds(0).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '1 day' * -10",
                       Period.days(-10).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '1 year' * 2147483647",
                       Period.years(2147483647).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '1 year' * -2147483648",
                       Period.years(-2147483648).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    public void test_normalize_multiplication_result() {
        assertEvaluate("900 * interval '1 second'",
                       Period.minutes(15).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("interval '1 second' * 900",
                       Period.minutes(15).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_implicit_cast_to_integer_while_multiplying_by_double() {
        assertEvaluate("interval '1 hour' * 3.5",
                       Period.hours(3).withPeriodType(PeriodType.yearMonthDayTime()));
        assertEvaluate("3.5 * interval '1 hour'",
                       Period.hours(3).withPeriodType(PeriodType.yearMonthDayTime()));
    }

    @Test
    public void test_multiplication_overflow() {
        assertThatThrownBy(() -> assertEvaluate("interval '2 second' * 2147483647", null))
                .isExactlyInstanceOf(ArithmeticException.class)
                .hasMessage("Multiplication overflows an int: 2 * 2147483647");
        assertThatThrownBy(() -> assertEvaluate("interval '2 second' * -2147483648", null))
                .isExactlyInstanceOf(ArithmeticException.class)
                .hasMessage("Multiplication overflows an int: 2 * -2147483648");
        assertThatThrownBy(() -> assertEvaluate("2147483647 * interval '2 second'", null))
                .isExactlyInstanceOf(ArithmeticException.class)
                .hasMessage("Multiplication overflows an int: 2 * 2147483647");
        assertThatThrownBy(() -> assertEvaluate("-2147483648 * interval '2 second'", null))
                .isExactlyInstanceOf(ArithmeticException.class)
                .hasMessage("Multiplication overflows an int: 2 * -2147483648");
    }

    @Test
    public void test_out_of_range_value() {
        assertThatThrownBy(() -> assertEvaluate("interval '9223372036854775807'", null))
            .isExactlyInstanceOf(ArithmeticException.class)
            .hasMessageStartingWith("Interval field value out of range");
    }

    @Test
    public void test_null_interval() {
        assertEvaluateNull("null + interval '1 second'");
        assertEvaluateNull("null - interval '1 second'");
        assertEvaluateNull("interval '1 second' + null");
        assertEvaluateNull("interval '1 second' - null");
        assertEvaluateNull("null * interval '1 second'");
        assertEvaluateNull("interval '1 second' * null");
    }

    @Test
    public void test_unsupported_arithmetic_operator_on_interval_types() {
        assertThatThrownBy(
            () -> assertEvaluate("null / interval '1 second'", null))
                .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (NULL / cast('1 second' AS interval)), " +
                                    "no overload found for matching argument types: (undefined, interval).");
    }

    @Test
    public void test_timestamp_interval() {
        assertEvaluate("interval '1 second' + '86400000'::timestamp", 86401000L);
        assertEvaluate("'86401000'::timestamp - interval '1 second'", 86400000L);
        assertEvaluate("'86400000'::timestamp - interval '-1 second'", 86401000L);
        assertEvaluate("'86400000'::timestamp + interval '-1 second'", 86399000L);
        assertEvaluate("'86400000'::timestamp - interval '1000 years'", -31556822400000L);
        assertEvaluate("'9223372036854775807'::timestamp - interval '1 second'", 9223372036854774807L);
    }

    @Test
    public void test_unallowed_operations() {
        assertThatThrownBy(
            () -> assertEvaluate("interval '1 second' - '86401000'::timestamptz", 86400000L))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (cast('1 second' AS interval) - cast('86401000' AS timestamp with time zone)), ");
    }

    @Test
    public void test_date_interval(){
        assertEvaluate("interval '1 second' + '86400000'::timestamp", 86401000L);
        assertEvaluate("'86400000'::timestamp + interval '1 second'", 86401000L);
        assertEvaluate("'86401000'::timestamp - interval '1 second'", 86400000L);

    }


}
