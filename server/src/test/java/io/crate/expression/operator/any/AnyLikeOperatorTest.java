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
        assertNormalize("'bar' like any (['%bar'])", isLiteral(true));
        assertNormalize("'foobar' like any (['%bar'])", isLiteral(true));
        assertNormalize("'bar' like any (['%ar', '%car'])", isLiteral(true));
        assertNormalize("'bar' like any (['%car', '%dar'])", isLiteral(false));
        assertNormalize("'foobar' like any (['%oob%'])", isLiteral(true));
        assertNormalize("'FOObAr' ilike any (['%bar'])", isLiteral(true));
        assertNormalize("'bOOb' ilike any (['%oob%'])", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertNormalize("'bar' like any (['_ar'])", isLiteral(true));
        assertNormalize("'bar' like any (['_bar'])", isLiteral(false));
        assertNormalize("'foot' like any (['foo_'])", isLiteral(true));
        assertNormalize("'foo' like any (['foo_'])", isLiteral(false));
        assertNormalize("'foo' like any (['_o_'])", isLiteral(true));
        assertNormalize("'foobar' like any (['_foobar_'])", isLiteral(false));
        assertNormalize("'bAR' ilike any (['_ar'])", isLiteral(true));
        assertNormalize("'fOo' ilike any (['_o_'])", isLiteral(true));
        assertNormalize("'foo' ilike any (['fOO_'])", isLiteral(false));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'foobar' like any (['%o_ar'])", isLiteral(true));
        assertNormalize("'foobar' like any (['%o_a%'])", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like any (['%i%m%'])", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like any (['%%%sum%%'])", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like any (['%i%m'])", isLiteral(false));
        assertNormalize("'fOObar' ilike any (['%o_a%'])", isLiteral(true));
        assertNormalize("'Lorem IpSuM dolor...' ilike any (['%%%sum%%'])", isLiteral(true));
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
        assertEvaluate("'foobar' like any (['%o_ar'])", true);
        assertEvaluate("'foobar' like any (['%o_a%'])", true);
        assertEvaluate("'Lorem ipsum dolor...' like any (['%i%m%'])", true);
        assertEvaluate("'Lorem ipsum dolor...' like any (['%%%sum%%'])", true);
        assertEvaluate("'Lorem ipsum dolor...' like any (['%i%m'])", false);
        assertEvaluate("'fOObar' ilike any (['%o_a%'])", true);
        assertEvaluate("'Lorem IpSuM dolor...' ilike any (['%%%sum%%'])", true);
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

    @Test
    public void test_only_rhs_arg_can_be_pattern() {
        assertEvaluate("'a%' like any(['a__'])", false);
    }
}
