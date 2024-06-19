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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.Asserts.isNull;
import static io.crate.testing.Asserts.isSameInstance;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class DateTruncFunctionTest extends ScalarTestCase {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Literal<Long> TIMESTAMP = Literal.of(DataTypes.TIMESTAMPZ, 919946281123L);

    @Test
    public void testDateTruncWithLongLiteral() throws Exception {
        assertNormalize("date_trunc('day', 1401777485000)", isLiteral(1401753600000L));
    }

    @Test
    public void testDateTruncWithStringLiteral() throws Exception {
        assertNormalize("date_trunc('day', '2014-06-03')", isLiteral(1401753600000L));
    }

    @Test
    public void test_date_trunc_works_with_timestamp_without_timezone() throws Exception {
        assertNormalize(
            "date_trunc('day', cast('2014-06-03' as timestamp without time zone))",
            isLiteral(1401753600000L)
        );
    }

    @Test
    public void testNormalizeNullInterval() throws Exception {
        assertNormalize("date_trunc(null, 919946281123)", isLiteral(null));
    }

    @Test
    public void testEvaluateNullInterval() throws Exception {
        assertEvaluateNull("date_trunc(interval, 919946281123)", Literal.of((String) null));
    }

    @Test
    public void testInvalidInterval() throws Exception {
        assertThatThrownBy(() -> {
            assertNormalize("date_trunc('invalid interval', 919946281123)", isNull());

        })
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid interval 'invalid interval' for scalar 'date_trunc'");
    }

    @Test
    public void testNullTimestamp() throws Exception {
        assertEvaluateNull("date_trunc('second', timestamp_tz)", Literal.of(DataTypes.TIMESTAMPZ, null));
        assertEvaluateNull("date_trunc('second', 'UTC', timestamp_tz)", Literal.of(DataTypes.TIMESTAMPZ, null));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("date_trunc('second', timestamp_tz)",
                       919946281000L,
                       TIMESTAMP);  // Thu Feb 25 12:38:01.000 UTC 1999
        assertEvaluate("date_trunc('minute', timestamp_tz)",
                       919946280000L,
                       TIMESTAMP);  // Thu Feb 25 12:38:00.000 UTC 1999
        assertEvaluate("date_trunc('hour', timestamp_tz)",
                       919944000000L,
                       TIMESTAMP);    // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', timestamp_tz)",
                       919900800000L,
                       TIMESTAMP);     // Thu Feb 25 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('week', timestamp_tz)",
                       919641600000L,
                       TIMESTAMP);    // Mon Feb 22 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('month', timestamp_tz)",
                       917827200000L,
                       TIMESTAMP);   // Mon Feb  1 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('year', timestamp_tz)",
                       915148800000L,
                       TIMESTAMP);    // Fri Jan  1 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('quarter', timestamp_tz)",
                       915148800000L,
                       TIMESTAMP); // Fri Jan  1 00:00:00.000 UTC 1999
    }

    @Test
    public void test_interval_is_case_insensitive() {
        assertEvaluate("date_trunc('dAy', timestamp_tz)", 919900800000L, TIMESTAMP);
    }


    @Test
    public void testDateTruncWithLongDataType() throws Exception {
        assertEvaluate("date_trunc('day', 'Europe/Vienna', x)", 1401746400000L,
            Literal.of(1401777485000L));
    }

    @Test
    public void testDateTruncWithStringLiteralTzAware() throws Exception {
        assertNormalize("date_trunc('day', 'Europe/Vienna', '2014-06-03')", isLiteral(1401746400000L));
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        assertThatThrownBy(() -> {
            assertNormalize("date_trunc('day', 'no time zone', 919946281123)", isNull());

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid time zone value 'no time zone'");
    }

    @Test
    public void testEvaluateTimeZoneAware() throws Exception {
        assertEvaluate("date_trunc('hour', 'Europe/Vienna', timestamp_tz)",
                       919944000000L,
                       TIMESTAMP); // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('hour', 'CET', timestamp_tz)",
                       919944000000L,
                       TIMESTAMP);           // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', 'UTC', timestamp_tz)",
                       919900800000L,
                       TIMESTAMP);            // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', 'Europe/Moscow', timestamp_tz)",
                       919890000000L,
                       TIMESTAMP);  // Wed Feb 24 21:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', '+01:00', timestamp_tz)",
                       919897200000L,
                       TIMESTAMP);         // Wed Feb 24 23:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', '+03:00', timestamp_tz)",
                       919890000000L,
                       TIMESTAMP);         //  Wed Feb 24 21:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', '-08:00', timestamp_tz)",
                       919929600000L,
                       TIMESTAMP);         //  Thu Feb 25 08:00:00.000 UTC 1999
    }

    @Test
    public void testCompile() throws Exception {
        assertCompile("date_trunc(interval, timezone, timestamp_tz)", isSameInstance());
        assertCompile("date_trunc('day', 'UTC', timestamp_tz)", isNotSameInstance());
    }
}
