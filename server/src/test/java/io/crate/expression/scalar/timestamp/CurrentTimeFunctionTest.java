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

package io.crate.expression.scalar.timestamp;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.SystemClock;
import io.crate.types.TimeTZ;

public class CurrentTimeFunctionTest extends ScalarTestCase {

    private static final long CURRENT_TIME_MILLIS = (10 * 3600 + 57 * 60 + 12) * 1000L; // 10:57:12 UTC

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(CURRENT_TIME_MILLIS);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    private static TimeTZ microsFromMidnightUTC(Instant i) {
        long dateTimeMicros = (i.getEpochSecond() * 1000_000_000L + i.getNano()) / 1000L;
        long dateMicros = i.truncatedTo(ChronoUnit.DAYS).toEpochMilli() * 1000L;
        return new TimeTZ(dateTimeMicros - dateMicros, 0);
    }

    @Test
    public void time_is_created_correctly() {
        TimeTZ expected = microsFromMidnightUTC(txnCtx.currentInstant());
        assertEvaluate("current_time", expected);
        assertEvaluate("current_time(6)", expected);
    }

    @Test
    public void test_calls_within_statement_are_idempotent() {
        assertEvaluate("current_time = current_time", true);
    }

    @Test
    public void time_zero_precission() {
        assertEvaluate("current_time(0)", microsFromMidnightUTC(txnCtx.currentInstant()));
    }

    @Test
    public void precision_larger_than_6_raises_exception() {
        assertThatThrownBy(() -> assertEvaluateNull("current_time(14)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("precision must be between [0..6]");
    }

    @Test
    public void integerIsNormalizedToLiteral() {
        assertNormalize("current_time(1)", s -> assertThat(s).isExactlyInstanceOf(Literal.class));
    }
}
