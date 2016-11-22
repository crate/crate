/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operation.scalar;

import com.google.common.base.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.Scalar;
import io.crate.types.DataTypes;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import javax.annotation.Nullable;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.not;

public class DateTruncFunctionTest extends AbstractScalarFunctionsTest {

    // timestamp for Do Feb 25 12:38:01.123 UTC 1999
    private static final Literal TIMESTAMP = Literal.of(DataTypes.TIMESTAMP, 919946281123L);

    @Test
    public void testDateTruncWithLongLiteral() {
        assertNormalize("date_trunc('day', 1401777485000)", isLiteral(1401753600000L));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDateTruncWithStringLiteral() {
        assertNormalize("date_trunc('day', '2014-06-03')", isLiteral(1401753600000L));
    }

    @Test
    public void testNullInterval() throws Exception {
        String val = null;
        assertEvaluate("date_trunc(interval, 919946281123)", null, Literal.of(val));
    }

    @Test
    public void testInvalidInterval() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid interval 'invalid interval' for scalar 'date_trunc'");
        assertNormalize("date_trunc('invalid interval', 919946281123)", null);
    }

    @Test
    public void testNullTimestamp() throws Exception {
        Long nullLong = null;
        assertEvaluate("date_trunc('second', timestamp)", null, Literal.of(DataTypes.TIMESTAMP, nullLong));
        assertEvaluate("date_trunc('second', 'UTC', timestamp)", null, Literal.of(DataTypes.TIMESTAMP, nullLong));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("date_trunc('second', timestamp)", 919946281000L, TIMESTAMP);  // Thu Feb 25 12:38:01.000 UTC 1999
        assertEvaluate("date_trunc('minute', timestamp)", 919946280000L, TIMESTAMP);  // Thu Feb 25 12:38:00.000 UTC 1999
        assertEvaluate("date_trunc('hour', timestamp)", 919944000000L, TIMESTAMP);    // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', timestamp)", 919900800000L, TIMESTAMP);     // Thu Feb 25 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('week', timestamp)", 919641600000L, TIMESTAMP);    // Mon Feb 22 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('month', timestamp)", 917827200000L, TIMESTAMP);   // Mon Feb  1 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('year', timestamp)", 915148800000L, TIMESTAMP);    // Fri Jan  1 00:00:00.000 UTC 1999
        assertEvaluate("date_trunc('quarter', timestamp)", 915148800000L, TIMESTAMP); // Fri Jan  1 00:00:00.000 UTC 1999
    }


    @Test
    public void testDateTruncWithLongDataType() {
        assertEvaluate("date_trunc('day', 'Europe/Vienna', x)", 1401746400000L,
            Literal.of(1401777485000L));
    }

    @Test
    public void testDateTruncWithStringLiteralTzAware() {
        assertNormalize("date_trunc('day', 'Europe/Vienna', '2014-06-03')", isLiteral(1401746400000L));
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid time zone value 'no time zone'");
        assertNormalize("date_trunc('day', 'no time zone', 919946281123)", null);
    }

    @Test
    public void testEvaluateTimeZoneAware() throws Exception {
        assertEvaluate("date_trunc('hour', 'Europe/Vienna', timestamp)", 919944000000L, TIMESTAMP); // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('hour', 'CET', timestamp)", 919944000000L, TIMESTAMP);           // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', 'UTC', timestamp)", 919900800000L, TIMESTAMP);            // Thu Feb 25 12:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', 'Europe/Moscow', timestamp)", 919890000000L, TIMESTAMP);  // Wed Feb 24 21:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', '+01:00', timestamp)", 919897200000L, TIMESTAMP);         // Wed Feb 24 23:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', '+03:00', timestamp)", 919890000000L, TIMESTAMP);         //  Wed Feb 24 21:00:00.000 UTC 1999
        assertEvaluate("date_trunc('day', '-08:00', timestamp)", 919929600000L, TIMESTAMP);         //  Thu Feb 25 08:00:00.000 UTC 1999
    }

    @Test
    public void testCompile() throws Exception {
        assertCompile("date_trunc(interval, timezone, timestamp)", new Function<Scalar, Matcher<Scalar>>() {
            @Nullable
            @Override
            public Matcher<Scalar> apply(@Nullable Scalar input) {
                return IsSame.sameInstance(input);
            }
        }, Literal.of("day"), Literal.of("UTC"), TIMESTAMP);
        assertCompile("date_trunc('day', 'UTC', timestamp)", new Function<Scalar, Matcher<Scalar>>() {
            @Nullable
            @Override
            public Matcher<Scalar> apply(@Nullable Scalar input) {
                return not(IsSame.sameInstance(input));
            }
        }, TIMESTAMP);
    }
}
