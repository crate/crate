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

import java.time.LocalDate;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.SystemClock;


public class CurrentDateFunctionTest extends ScalarTestCase {

    private long currentTimestamp = 1422294644581L;
    private LocalDate currentDate = LocalDate.of(2015, 1, 26);

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(currentTimestamp);
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
        assertEvaluate("CURDATE()", currentDate);
    }

    @Test
    public void testCurrentDateReturnsExpectedDate() {
        assertEvaluate("CURRENT_DATE", currentDate);
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
