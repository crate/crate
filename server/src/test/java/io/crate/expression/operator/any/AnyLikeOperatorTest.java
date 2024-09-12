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

package io.crate.expression.operator.any;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class AnyLikeOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSimpleSymbolEqual() {
        assertNormalize("'foo' like any (['foo'])", isLiteral(true));
        assertNormalize("'notFoo' like any (['foo'])", isLiteral(false));
        assertNormalize("'foo' ilike any (['fOo', 'FOO'])", isLiteral(true));
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
        assertNormalize("'%bar' ilike any (['FOObAr'])", isLiteral(true));
        assertNormalize("'foo%' ilike any (['fO', 'kuh'])", isLiteral(false));
        assertNormalize("'%oob%' ilike any (['fobar', 'bOOb'])", isLiteral(true));
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
        assertNormalize("'_ar' ilike any (['bAR'])", isLiteral(true));
        assertNormalize("'_o_' ilike any (['fOo'])", isLiteral(true));
        assertNormalize("'fOO_' ilike any (['foo'])", isLiteral(false));
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
        assertNormalize("'%o_a%' ilike any (['fOObar'])", isLiteral(true));
        assertNormalize("'%%%sum%%' ilike any (['Lorem IpSuM dolor...'])", isLiteral(true));
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
        assertEvaluate("'%o_a%' ilike any (['fOObar'])", true);
        assertEvaluate("'%%%sum%%' ilike any (['Lorem IpSuM dolor...'])", true);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluateNull("null like any([null])");
        assertEvaluateNull("'foo'like any([null])");
        assertEvaluateNull("null like any(['bar'])");
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null like any([null])", isLiteral(null));
        assertNormalize("'foo'like any([null])", isLiteral(null));
        assertNormalize("null like any(['bar'])", isLiteral(null));

        assertNormalize("null ilike any([null])", isLiteral(null));
        assertNormalize("'foo'ilike any([null])", isLiteral(null));
        assertNormalize("null ilike any(['bar'])", isLiteral(null));
    }

    @Test
    public void testNegateLike() throws Exception {
        assertEvaluate("not 'A' like any (['A', 'B'])", false);
        assertEvaluate("not 'A' ilike any (['a', 'b'])", false);
    }

    @Test
    public void test_patterns_on_right_arg() {
        assertNormalize("'foobar' like any (['%bar'])", isLiteral(true));
        assertNormalize("'bar' like any (['_ar'])", isLiteral(true));
        assertNormalize("'foobar' like any (['%o_a%'])", isLiteral(true));
        assertNormalize("'fOobAR' ilike any (['%BaR'])", isLiteral(true));
        assertNormalize("'BaR' ilike any (['_ar'])", isLiteral(true));
        assertNormalize("'foobar' ilike any (['%O_a%'])", isLiteral(true));
    }

    @Test
    public void test_non_string_values() {
        assertNormalize("1 like any ([1, null, 2])", isLiteral(true));
        assertNormalize("1 not like any ([1])", isLiteral(false));
        assertNormalize("1 ilike any ([1, null, 2])", isLiteral(true));
        assertNormalize("1 not ilike any ([1])", isLiteral(false));
    }

    @Test
    public void test_wildcard_escaped_in_c_style_string() {
        assertEvaluate("'TextToMatch' LIKE ANY ([E'Te\\%tch'])", true);
        assertEvaluate("'TextToMatch' ILIKE ANY ([E'te\\%tch'])", true);
    }

    @Test
    public void test_any_like_ilike_with_trailing_escape_character() {
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' LIKE ANY (['a', 'b', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' ILIKE ANY (['a', 'b', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
    }
}
