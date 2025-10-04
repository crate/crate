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

package io.crate.expression.scalar.formatting;

import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ABBREVIATED_DAY_CAPITALIZED;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ABBREVIATED_DAY_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ABBREVIATED_DAY_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ABBREVIATED_MONTH_CAPITALIZED;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ABBREVIATED_MONTH_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ABBREVIATED_MONTH_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.AD_ERA_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.AD_ERA_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.AM_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.AM_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.A_D_ERA_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.A_D_ERA_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.A_M_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.A_M_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.BC_ERA_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.BC_ERA_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.B_C_ERA_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.B_C_ERA_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.DAY_CAPITALIZED;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.DAY_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.DAY_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.MONTH_CAPITALIZED;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.MONTH_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.MONTH_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.PM_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.PM_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.P_M_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.P_M_UPPER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ROMAN_MONTH_LOWER;
import static io.crate.expression.scalar.formatting.DateTimeFormatter.Token.ROMAN_MONTH_UPPER;
import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.Asserts.isSameInstance;

import java.util.Locale;
import java.util.Set;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;


public class ToCharFunctionTest extends ScalarTestCase {

    // Patterns that must NOT behave in the same way for upper/lower case,
    private static final Set<DateTimeFormatter.Token> FIXED_CASE_PATTERNS =
        Set.of(
            MONTH_UPPER,
            MONTH_CAPITALIZED,
            MONTH_LOWER,
            ABBREVIATED_MONTH_UPPER,
            ABBREVIATED_MONTH_CAPITALIZED,
            ABBREVIATED_MONTH_LOWER,
            DAY_UPPER,
            DAY_CAPITALIZED,
            DAY_LOWER,
            ABBREVIATED_DAY_UPPER,
            ABBREVIATED_DAY_CAPITALIZED,
            ABBREVIATED_DAY_LOWER,
            AM_UPPER,
            AM_LOWER,
            PM_UPPER,
            PM_LOWER,
            A_M_UPPER,
            A_M_LOWER,
            P_M_UPPER,
            P_M_LOWER,
            BC_ERA_UPPER,
            BC_ERA_LOWER,
            AD_ERA_UPPER,
            AD_ERA_LOWER,
            B_C_ERA_UPPER,
            B_C_ERA_LOWER,
            A_D_ERA_UPPER,
            A_D_ERA_LOWER,
            ROMAN_MONTH_UPPER,
            ROMAN_MONTH_LOWER
        );

    @Test
    public void testEvaluateTimestamp() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12.12345', 'Day,  DD  HH12:MI:SS')",
            "Thursday,  01  05:31:12"
        );
    }

    @Test
    public void test_lower_case_patterns() throws Exception {
        assertEvaluate("to_char('2024-12-13'::timestamp, 'yyyy-mm-dd')", "2024-12-13");
        assertEvaluate("to_char('2024-12-13'::timestamp, 'mm')", "12");
        assertEvaluate("to_char('2024-12-13'::timestamp, 'miss-')", "0000-");
        assertEvaluate("to_char('2024-12-13'::timestamp, 'd')", "6");

        assertEvaluate("to_char('2024-12-13'::timestamp, 'w')", "2");
        assertEvaluate("to_char('2024-12-13'::timestamp, 'cc')", "21");
        assertEvaluate("to_char('2024-12-13'::timestamp, 'j')", "2460658");
        assertEvaluate("to_char('2024-12-13'::timestamp, 'iw')", "50");
    }

    @Test
    public void test_lower_case_same_result_as_upper_case_for_non_mixed_case_patterns() throws Exception {
        for (DateTimeFormatter.Token token: DateTimeFormatter.Token.values()) {
            var tokenValue = token.toString();
            if (FIXED_CASE_PATTERNS.contains(token) == false && Character.isLowerCase(tokenValue.charAt(0))) {
                assertEvaluate(
                    "to_char(timestamp '1970-01-01', name) = to_char(timestamp '1970-01-01', name)",
                    true,
                    Literal.of(tokenValue),
                    Literal.of(tokenValue.toUpperCase(Locale.ENGLISH))
                );
            }
        }
    }

    @Test
    public void test_lower_case_yyyy_supported() throws Exception {
        assertEvaluate("to_char(timestamp '1970-01-01', 'yyyy')", "1970");
        assertEvaluate("to_char(timestamp '1971-01-01T17:31:12', 'yyyy')", "1971");
        assertEvaluate("to_char(interval '2 year', 'yyyy')", "0002");
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds', 'yyyy')", "0001");
    }

    @Test
    public void testEvaluateTimestampWithNullPattern() {
        assertEvaluateNull("to_char(timestamp '1970-01-01T17:31:12', null)");
    }

    @Test
    public void testEvaluateNullExpression() {
        assertEvaluateNull("to_char(null, 'EEEE, LLLL d - h:m a uuuu G')");
    }

    @Test
    public void testEvaluateInterval() {
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds', 'YYYY MM DD HH12:MI:SS')", "0001 03 22 05:06:07");
    }

    @Test
    public void test_evaluate_interval_milliseconds() {
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds 1000 milliseconds', 'YYYY MM DD HH12:MI:SS')", "0001 03 22 05:06:08");
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds 334 milliseconds', 'YYYY MM DD HH12:MI:SS')", "0001 03 22 05:06:07");
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds 1000 milliseconds', 'YYYY MM DD HH12:MI:SS.MS')", "0001 03 22 05:06:08.000");
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds 334 milliseconds', 'YYYY MM DD HH12:MI:SS.MS')", "0001 03 22 05:06:07.334");
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds 1334 milliseconds', 'YYYY MM DD HH12:MI:SS.MS')", "0001 03 22 05:06:08.334");
    }

    @Test
    public void testEvaluateIntervalWithNullPattern() {
        assertEvaluateNull("to_char(timestamp '1970-01-01T17:31:12', null)");
    }

    @Test
    public void testCompileWithValues() throws Exception {
        assertCompile("to_char(timestamp, 'Day,  DD  HH12:MI:SS')", isNotSameInstance());
    }

    @Test
    public void testCompileWithRefs() throws Exception {
        assertCompile("to_char(timestamp, name)", isSameInstance());
    }
}
