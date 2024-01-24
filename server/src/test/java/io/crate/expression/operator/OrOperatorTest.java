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

import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class OrOperatorTest extends ScalarTestCase {

    @Test
    public void test_normalize_boolean_true_or_reference() throws Exception {
        assertNormalize("is_awesome or true", isLiteral(true));
    }

    @Test
    public void test_normalize_boolean_false_or_reference() throws Exception {
        assertNormalize("is_awesome or false", isReference("is_awesome"));
    }

    @Test
    public void testNormalize() throws Exception {
        assertNormalize("1 or true", isLiteral(true));
        assertNormalize("true or 1", isLiteral(true));
        assertNormalize("false or 1", isLiteral(true));
        assertNormalize("false or 0", isLiteral(false));
        assertNormalize("1 or 1", isLiteral(true));
        assertNormalize("0 or 1", isLiteral(true));
        assertNormalize("true or (1/0 = 10)", isLiteral(true));
        assertNormalize("(1/0 = 10) or true", isLiteral(true));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("true or true", true);
        assertEvaluate("false or false", false);
        assertEvaluate("true or false", true);
        assertEvaluate("false or true", true);
        assertEvaluate("true or null", true);
        assertEvaluate("null or true", true);
        assertEvaluateNull("false or null");
        assertEvaluateNull("null or false");
        assertEvaluateNull("null or null");
    }
}
