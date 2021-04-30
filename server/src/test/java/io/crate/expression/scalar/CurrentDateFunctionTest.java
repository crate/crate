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

import io.crate.metadata.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;


public class CurrentDateFunctionTest extends ScalarTestCase {

    private static final long CURRENT_TIMESTAMP = 1422294644581L;

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(CURRENT_TIMESTAMP);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void testCurdateReturnsSameValueAsDayTrunc() {
        assertEvaluate("CURDATE() = DATE_TRUNC('day', CURRENT_TIMESTAMP)", true);
    }

    @Test
    public void testCurdateReturnsExpectedDate() {
        var dayTruncMillis = LocalDate.ofInstant(Instant.ofEpochMilli(CURRENT_TIMESTAMP), ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEvaluate("CURDATE()", dayTruncMillis);
    }

    @Test
    public void testCurrentDateReturnsExpectedDate() {
        var dayTruncMillis = LocalDate.ofInstant(Instant.ofEpochMilli(CURRENT_TIMESTAMP), ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        assertEvaluate("CURRENT_DATE", dayTruncMillis);
    }

    @Test
    public void testCurdateCallsWithinStatementAreIdempotent() {
        assertEvaluate("CURDATE() = CURDATE()", true);
    }

    @Test
    public void testCurrentDateCallsWithinStatementAreIdempotent() {
        assertEvaluate("CURRENT_DATE = CURRENT_DATE", true);
    }

}
