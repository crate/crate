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

public class IsOperatorTest extends ScalarTestCase {

    @Test
    public void testIsTrue() {
        assertEvaluate("true is true", true);
        assertEvaluate("false is true", false);
        assertEvaluate("null is true", false);
    }

    @Test
    public void testIsFalse() {
        assertEvaluate("true is false", false);
        assertEvaluate("false is false", true);
        assertEvaluate("null is false", false);
    }


    @Test
    public void testIsNotTrue() {
        assertEvaluate("true is not true", false);
        assertEvaluate("false is not true", true);
        assertEvaluate("null is not true", true);
    }

    @Test
    public void testIsNotFalse() {
        assertEvaluate("true is not false", true);
        assertEvaluate("false is not false", false);
        assertEvaluate("null is not false", true);
    }

    @Test
    public void testNormalizeSymbol() {
        assertNormalize("true is true", isLiteral(true));
        assertNormalize("null is true", isLiteral(false));
        assertNormalize("null is false", isLiteral(false));

        assertNormalize("true is not true", isLiteral(false));
        assertNormalize("null is not true", isLiteral(true));
        assertNormalize("null is not false", isLiteral(true));
    }

    @Test
    public void testNormalizeReference() {
        assertNormalize("name::boolean is true", isFunction(IsOperator.NAME));
        assertNormalize("name::boolean is false", isFunction(IsOperator.NAME));
    }
}
