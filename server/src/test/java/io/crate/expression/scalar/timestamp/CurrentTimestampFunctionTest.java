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

import static io.crate.testing.Asserts.exactlyInstanceOf;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.SystemClock;

public class CurrentTimestampFunctionTest extends ScalarTestCase {

    private static final long EXPECTED_TIMESTAMP = 1422294644581L;

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(EXPECTED_TIMESTAMP);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void timestampIsCreatedCorrectly() {
        assertEvaluate("current_timestamp", EXPECTED_TIMESTAMP);
    }

    @Test
    public void precisionOfZeroDropsAllFractionsOfSeconds() {
        assertEvaluate("current_timestamp(0)", EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 1000));
    }

    @Test
    public void precisionOfOneDropsLastTwoDigitsOfFractionsOfSecond() {
        assertEvaluate("current_timestamp(1)", EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 100));
    }

    @Test
    public void precisionOfTwoDropsLastDigitOfFractionsOfSecond() {
        assertEvaluate("current_timestamp(2)", EXPECTED_TIMESTAMP - (EXPECTED_TIMESTAMP % 10));
    }

    @Test
    public void precisionOfThreeKeepsAllFractionsOfSeconds() {
        assertEvaluate("current_timestamp(3)", EXPECTED_TIMESTAMP);
    }

    @Test
    public void precisionLargerThan3RaisesException() {
        assertThatThrownBy(() -> assertEvaluateNull("current_timestamp(4)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Precision must be between 0 and 3");
    }

    @Test
    public void test_calls_within_statement_are_idempotent() {
        assertEvaluate("current_timestamp(3) = current_timestamp(3)", true);
    }

    @Test
    public void integerIsNormalizedToLiteral() {
        assertNormalize("current_timestamp(1)", exactlyInstanceOf(Literal.class));
    }
}
