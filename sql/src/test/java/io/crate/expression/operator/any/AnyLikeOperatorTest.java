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

package io.crate.expression.operator.any;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class AnyLikeOperatorTest extends AbstractScalarFunctionsTest  {

    @Test
    public void testNormalizeSingleSymbolEqual() {
        assertNormalize("'foo' like any (['foo'])", isLiteral(true));
        assertNormalize("'notFoo' like any (['foo'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertNormalize("'%bar' like any (['foobar', 'bar'])", isLiteral(true));
        assertNormalize("'%bar' like any (['bar'])", isLiteral(true));
        assertNormalize("'%bar' like any (['ar', 'car'])", isLiteral(false));
        assertNormalize("'foo%' like any (['foobar', 'kuhbar'])", isLiteral(true));
        assertNormalize("'foo%' like any (['foo', 'kuh'])", isLiteral(true));
        assertNormalize("'foo%' like any (['fo', 'kuh'])", isLiteral(false));
        assertNormalize("'%oob%' like any (['foobar'])", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertNormalize("'_ar' like any (['bar'])", isLiteral(true));
        assertNormalize("'_bar' like any (['bar'])", isLiteral(false));
        assertNormalize("'fo_' like any (['bar', 'for'])", isLiteral(true));
        assertNormalize("'foo_' like any (['foo', 'foot'])", isLiteral(true));
        assertNormalize("'foo_' like any (['foo'])", isLiteral(false));
        assertNormalize("'_o_' like any (['foo'])", isLiteral(true));
        assertNormalize("'_foobar_' like any (['foobar'])", isLiteral(false));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'%o_ar' like any (['foobar', 'foobaz'])", isLiteral(true));
        assertNormalize("'%a_' like any (['foobar'])", isLiteral(true));
        assertNormalize("'%o_a%' like any (['foobar'])", isLiteral(true));
        assertNormalize("'%i%m%' like any (['Lorem ipsum dolor...'])", isLiteral(true));
        assertNormalize("'%%%sum%%' like any (['Lorem ipsum dolor...'])", isLiteral(true));
        assertNormalize("'%i%m' like any (['Lorem ipsum dolor...'])", isLiteral(false));
    }

    @Test
    public void testEvaluateStraight() throws Exception {
        assertEvaluate("'foo' like any (['foo', 'koo', 'doo'])", true);
        assertEvaluate("'foo' like any (['foo'])", true);
        assertEvaluate("'foo' like any ([])", false);
        assertEvaluate("'foo' like any (['koo', 'doo'])", false);
    }

    @Test
    public void testEvaluateLikeMixed() {
        assertEvaluate("'%o_ar' like any (['foobar', 'foobaz'])", true);
        assertEvaluate("'%a_' like any (['foobar'])", true);
        assertEvaluate("'%o_a%' like any (['foobar'])", true);
        assertEvaluate("'%i%m%' like any (['Lorem ipsum dolor...'])", true);
        assertEvaluate("'%%%sum%%' like any (['Lorem ipsum dolor...'])", true);
        assertEvaluate("'%i%m' like any (['Lorem ipsum dolor...'])", false);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluate("null like any([null])", null);
        assertEvaluate("'foo'like any([null])", null);
        assertEvaluate("null like any(['bar'])", null);
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null like any([null])", isLiteral(null));
        assertNormalize("'foo'like any([null])", isLiteral(null));
        assertNormalize("null like any(['bar'])", isLiteral(null));
    }

    @Test
    public void testNegateLike() throws Exception {
        assertEvaluate("not 'A' like any (['A', 'B'])", false);
    }
}
