/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import org.junit.Test;


public class ToCharFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluateTimestamp() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12', 'EEEE, LLLL d - h:m a uuuu G')",
            "Thursday, January 1 - 5:31 PM 1970 AD"
        );
    }

    @Test
    public void testEvaluateTimestampWithInvalidPattern() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown pattern letter: B");
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12', 'ABC')",
            null
        );
    }

    @Test
    public void testEvaluateTimestampWithNullPattern() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12', null)",
            null
        );
    }

    @Test
    public void testEvaluateNullExpression() {
        assertEvaluate(
            "to_char(null, 'EEEE, LLLL d - h:m a uuuu G')",
            null
        );
    }

    @Test
    public void testEvaluateInterval() {
        assertEvaluate("to_char(INTERVAL '1-2 3 4:5:6', 'HH24:M:SS')", "1 year, 2 months, 3 days, 4 hours, 5 minutes and 6 seconds");
    }

    @Test
    public void testEvaluateIntervalWithNullPattern() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12', null)",
            null
        );
    }

    @Test
    public void testEvaluateNumeric() {
        assertEvaluate("to_char(12345.678, '###,###.##')", "12,345.68");
        assertEvaluate("to_char(125.6::real, '0000')", "0126");
        assertEvaluate("to_char(-125.8, '###.00')", "-125.80");
    }

    @Test
    public void testEvaluateNumericWithEmptyPattern() {
        assertEvaluate("to_char(12345.678, '')", "12,345.678");
        assertEvaluate("to_char(-125.8, '')", "-125.8");
    }

    @Test
    public void testEvaluateNumericWithNullPattern() {
        assertEvaluate("to_char(12345.678, null)", null);
        assertEvaluate("to_char(125.6::real, null)", null);
        assertEvaluate("to_char(-125.8, null)", null);
    }

}
