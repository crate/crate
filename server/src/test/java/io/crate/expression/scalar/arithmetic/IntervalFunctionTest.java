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

import io.crate.expression.scalar.ScalarTestCase;
import org.hamcrest.Matchers;
import org.joda.time.Period;
import org.junit.Test;

public class IntervalFunctionTest extends ScalarTestCase {

    @Test
    public void test_interval_to_interval() {
        assertEvaluate("interval '1 second' + interval '1 second'", Period.seconds(2));
        assertEvaluate("interval '1100 years' + interval '2000 years'", Period.years(3100));
        assertEvaluate("interval '-10 years' + interval '1 years'", Period.years(-9));
        assertEvaluate("interval '2 second' - interval '1 second'", Matchers.is(Period.seconds(1)));
        assertEvaluate("interval '-1 second' - interval '-1 second'", Matchers.is(Period.seconds(0)));
        assertEvaluate("interval '1 month' + interval '1 year'", Matchers.is(Period.years(1).withMonths(1)));
    }

    @Test
    public void test_out_of_range_value() {
        expectedException.expect(ArithmeticException.class);
        expectedException.expectMessage("Interval field value out of range");
        assertEvaluate("interval '9223372036854775807'", Matchers.is(Period.seconds(1)));
    }

    @Test
    public void test_null_interval() {
        assertEvaluate("null + interval '1 second'", Matchers.nullValue());
        assertEvaluate("null - interval '1 second'", Matchers.nullValue());
        assertEvaluate("interval '1 second' + null", Matchers.nullValue());
        assertEvaluate("interval '1 second' - null", Matchers.nullValue());
    }

    @Test
    public void test_unsupported_arithmetic_operator_on_interval_types() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: (NULL * cast('1 second' AS interval))," +
                                        " no overload found for matching argument types: (undefined, interval).");
        assertEvaluate("null * interval '1 second'", Matchers.nullValue());
    }

    @Test
    public void test_timestamp_interval() {
        assertEvaluate("interval '1 second' + '86400000'::timestamp", Matchers.is(86401000L));
        assertEvaluate("'86401000'::timestamp - interval '1 second'", Matchers.is(86400000L));
        assertEvaluate("'86400000'::timestamp - interval '-1 second'", Matchers.is(86401000L));
        assertEvaluate("'86400000'::timestamp + interval '-1 second'", Matchers.is(86399000L));
        assertEvaluate("'86400000'::timestamp - interval '1000 years'", Matchers.is(-31556822400000L));
        assertEvaluate("'9223372036854775807'::timestamp - interval '1 second'", Matchers.is(9223372036854774807L));
    }

    @Test
    public void test_unallowed_operations() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: (cast('1 second' AS interval) - cast('86401000' AS timestamp with time zone))," +
                                        " no overload found for matching argument types: (interval, timestamp with time zone).");
        assertEvaluate("interval '1 second' - '86401000'::timestamp", Matchers.is(86400000L));
    }
}
