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

package io.crate.expression.scalar.arithmetic;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import org.hamcrest.Matchers;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class LogFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeValueSymbol() throws Exception {
        // test log(x) ... implicit base of 10
        assertNormalize("log(10.0)", isLiteral(1.0));
        assertNormalize("log(10)", isLiteral(1.0));
        assertNormalize("log(null)", isLiteral(null));

        assertNormalize("ln(1.0)", isLiteral(0.0));
        assertNormalize("ln(1)", isLiteral(0.0));
        assertNormalize("ln(null)", isLiteral(null));

        // test log(x, b) ... explicit base
        assertNormalize("log(10.0, 10.0)", isLiteral(1.0));
        assertNormalize("log(10, 10.0)", isLiteral(1.0));
        assertNormalize("log(10.0, 10)", isLiteral(1.0));
        assertNormalize("log(10, 10)", isLiteral(1.0));
        assertNormalize("log(null, 10)", isLiteral(null));
        assertNormalize("log(10, null)", isLiteral(null));
        assertNormalize("log(null, null)", isLiteral(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogZero() throws Exception {
        // -Infinity
        assertEvaluate("log(0.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogNegative() throws Exception {
        // NaN
        assertEvaluate("log(-10.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLnZero() throws Exception {
        // -Infinity
        assertEvaluate("ln(0.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLnNegative() throws Exception {
        // NaN
        assertEvaluate("ln(-10.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogDivisionByZero() throws Exception {
        // division by zero
        assertEvaluate("log(10.0, 1.0)", null);
    }

    @Test
    public void testNormalizeString() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'foo'` of type `text` to type `double precision`");
        assertNormalize("log('foo')", Matchers.nullValue());
    }

    @Test
    public void testLogInteger() throws Exception {
        assertEvaluate("log(x)", 1.0, Literal.of(10));
    }

    @Test
    public void testEvaluateLog10() throws Exception {
        assertEvaluate("log(100)", 2.0);
        assertEvaluate("log(100.0)", 2.0);
        assertEvaluate("log(null)", null);
    }

    @Test
    public void testEvaluateLogBase() throws Exception {
        assertEvaluate("log(10, 100)", 0.5);
        assertEvaluate("log(10.0, 100.0)", 0.5);
        assertEvaluate("log(10, 100.0)", 0.5);
        assertEvaluate("log(null, 10)", null);
        assertEvaluate("log(10, null)", null);
        assertEvaluate("log(null, null)", null);
    }

    @Test
    public void testEvaluateLn() throws Exception {
        assertEvaluate("ln(1)", 0.0);
        assertEvaluate("ln(1.0)", 0.0);
        assertEvaluate("ln(null)", null);
    }
}
