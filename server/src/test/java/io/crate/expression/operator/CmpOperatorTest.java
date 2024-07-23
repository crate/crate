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

package io.crate.expression.operator;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class CmpOperatorTest extends ScalarTestCase {

    @Test
    public void testLte() {
        assertNormalize("id <= 8", isFunction("op_<="));
        assertNormalize("8 <= 200", isLiteral(true));
        assertNormalize("0.1 <= 0.1", isLiteral(true));
        assertNormalize("16 <= 8", isLiteral(false));
        assertNormalize("'abc' <= 'abd'", isLiteral(true));
        assertNormalize("'2 hour'::interval <= '2 hour'::interval", isLiteral(true));
        assertNormalize("'1 hour'::interval <= '2 hour'::interval", isLiteral(true));
        assertNormalize("'1 ms'::interval <= '2 ms'::interval", isLiteral(true));
        assertNormalize("'2 ms'::interval <= '2 ms'::interval", isLiteral(true));
        assertNormalize("'1 month 1 hour'::interval <= '1 month 2 hour'::interval", isLiteral(true));
        assertNormalize("'987 years 567 months 482 days 127 hours 185 mins 31214 seconds'::interval <= " +
                        "'987 years 567 months 487 days 18 hours 45 mins 15 seconds'::interval",
                        isLiteral(true)); // manual normalize up to days
        assertNormalize("'987 years 567 months 482 days 127 hours 185 mins 31214 seconds'::interval <= " +
                        "'1035 years 7 months 2 days 18 hours 45 mins 13 seconds'::interval",
                        isLiteral(false)); // manual normalize up to years
        assertEvaluateNull("true <= null");
        assertEvaluateNull("null <= 1");
        assertEvaluateNull("null <= 'abc'");
        assertEvaluateNull("null <= null");
    }

    @Test
    public void testLt() {
        assertNormalize("id < 8", isFunction("op_<"));
        assertNormalize("0.1 < 0.2", isLiteral(true));
        assertNormalize("'abc' < 'abd'", isLiteral(true));
        assertNormalize("'1 hour'::interval < '2 hour'::interval", isLiteral(true));
        assertNormalize("'1 ms'::interval < '2 ms'::interval", isLiteral(true));
        assertNormalize("'1 month 1 hour'::interval < '1 month 2 hour'::interval", isLiteral(true));
        assertNormalize("'987 years 567 months 482 days 127 hours 185 mins 31214 seconds'::interval < " +
                        "'987 years 567 months 487 days 18 hours 45 mins 16 seconds'::interval",
                        isLiteral(true)); // manual normalize up to days
        assertNormalize("'987 years 567 months 482 days 127 hours 185 mins 31214 seconds'::interval < " +
                        "'1035 years 7 months 2 days 18 hours 45 mins 14 seconds'::interval",
                        isLiteral(false)); // manual normalize up to years
        assertEvaluateNull("true < null");
        assertEvaluateNull("null < 1");
        assertEvaluateNull("null < name", Literal.of("foo"));
        assertEvaluateNull("null < null");
    }

    @Test
    public void testGte() {
        assertNormalize("id >= 8", isFunction("op_>="));
        assertNormalize("0.1 >= 0.1", isLiteral(true));
        assertNormalize("16 >= 8", isLiteral(true));
        assertNormalize("'abc' >= 'abd'", isLiteral(false));
        assertNormalize("'2 hour'::interval >= '2 hour'::interval", isLiteral(true));
        assertNormalize("'2 ms'::interval >= '2 ms'::interval", isLiteral(true));
        assertNormalize("'0.002 secs'::interval >= '2 ms'::interval", isLiteral(true));
        assertNormalize("'12 hour'::interval >= '2 hour'::interval", isLiteral(true));
        assertNormalize("'1 month 1 hour'::interval >= '1 month 2 hour'::interval", isLiteral(false));
        assertEvaluateNull("true >= null");
        assertEvaluateNull("null >= 1");
        assertEvaluateNull("null >= 'abc'");
        assertEvaluateNull("null >= null");
    }

    @Test
    public void testGt() {
        assertNormalize("id > 200", isFunction("op_>"));
        assertNormalize("0.1 > 0.1", isLiteral(false));
        assertNormalize("16 > 8", isLiteral(true));
        assertNormalize("'abd' > 'abc'", isLiteral(true));
        assertNormalize("'10 hour'::interval > '2 hour'::interval", isLiteral(true));
        assertNormalize("'1 month 1 hour'::interval > '1 month 2 hour'::interval", isLiteral(false));
        assertEvaluateNull("true > null");
        assertEvaluateNull("null > 1");
        assertEvaluateNull("name > null", Literal.of("foo"));
        assertEvaluateNull("null > null");
    }

    @Test
    public void testBetween() {
        assertNormalize("0.1 between 0.01 and 0.2", isLiteral(true));
        assertNormalize("10 between 1 and 2", isLiteral(false));
        assertNormalize("'abd' between 'abc' and 'abe'", isLiteral(true));
        assertEvaluateNull("1 between 0 and null");
        assertEvaluateNull("1 between null and 10");
        assertEvaluateNull("1 between null and null");
        assertEvaluateNull("null between 1 and 10");
        assertEvaluateNull("null between 1 and null");
        assertEvaluateNull("null between null and 10");
        assertEvaluateNull("null between null and null");
    }
}
