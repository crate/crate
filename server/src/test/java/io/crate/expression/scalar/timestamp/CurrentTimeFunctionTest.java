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

package io.crate.expression.scalar.timestamp;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import io.crate.types.TimeTZ;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoField;

import static io.crate.expression.scalar.timestamp.TimezoneFunction.UTC;
import static org.hamcrest.Matchers.instanceOf;

public class CurrentTimeFunctionTest extends AbstractScalarFunctionsTest {

    private static final long CURRENT_TIME_MILLIS = (10 * 3600 + 57 * 60 + 12) * 1000L; // 10:57:12 UTC

    @Before
    public void prepare() {
        DateTimeUtils.setCurrentMillisFixed(CURRENT_TIME_MILLIS);
    }

    @After
    public void cleanUp() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void timeIsCreatedCorrectly() {
        long microsFromMidnight = Instant.ofEpochMilli(txnCtx.currentTimeMillis())
                                      .atZone(UTC)
                                      .getLong(ChronoField.NANO_OF_DAY) / 1000L;
        assertEvaluate("current_time", new TimeTZ(microsFromMidnight, 0));
    }

    @Test
    public void precision_larger_than_6_raises_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("precision must range between [0..6]");
        assertEvaluate("current_time(14)", null);
    }

    @Test
    public void integerIsNormalizedToLiteral() {
        assertNormalize("current_time(1)", instanceOf(Literal.class));
    }
}
