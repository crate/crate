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

public class AnyNotLikeOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSimpleSymbolEqual() {
        assertNormalize("'foo' not like any (['foo'])", isLiteral(false));
        assertNormalize("'notFoo' not like any (['foo'])", isLiteral(true));

        assertNormalize("'foo' not ilike any (['FoO'])", isLiteral(false));
        assertNormalize("'notFoo' not ilike any (['notFOO'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertNormalize("'bar' not like any (['%bar'])", isLiteral(false));
        assertNormalize("'foobar' not like any (['%bar'])", isLiteral(false));
        assertNormalize("'bar' not like any (['%ar', '%car'])", isLiteral(true));
        assertNormalize("'bar' not like any (['%car', '%dar'])", isLiteral(true));
        assertNormalize("'foobar' not like any (['%oob%'])", isLiteral(false));
        assertNormalize("'FOObAr' not ilike any (['%bar'])", isLiteral(false));
        assertNormalize("'bOOb' not ilike any (['%oob%'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertNormalize("'bar' not like any (['_ar'])", isLiteral(false));
        assertNormalize("'bar' not like any (['_bar'])", isLiteral(true));
        assertNormalize("'foot' not like any (['foo_'])", isLiteral(false));
        assertNormalize("'foo' not like any (['foo_'])", isLiteral(true));
        assertNormalize("'foo' not like any (['_o_'])", isLiteral(false));
        assertNormalize("'foobar' not like any (['_foobar_'])", isLiteral(true));
        assertNormalize("'bAR' not ilike any (['_ar'])", isLiteral(false));
        assertNormalize("'fOo' not ilike any (['_o_'])", isLiteral(false));
        assertNormalize("'foo' not ilike any (['fOO_'])", isLiteral(true));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'foobar' not like any (['%o_ar', '%o_a%'])", isLiteral(false));
        assertNormalize("'Lorem ipsum dolor...' not like any (['%i%m%'])", isLiteral(false));
        assertNormalize("'Lorem ipsum dolor...' not like any (['%%%sum%%'])", isLiteral(false));
        assertNormalize("'Lorem ipsum dolor...' not like any (['%i%m'])", isLiteral(true));
        assertNormalize("'fOObar' not ilike any (['%o_a%'])", isLiteral(false));
        assertNormalize("'Lorem IpSuM dolor...' not ilike any (['%%%sum%%'])", isLiteral(false));
    }

    @Test
    public void testEvaluateStraight() throws Exception {
        assertEvaluate("'foo' not like any (['foo', 'koo', 'doo'])", true);
        assertEvaluate("'foo' not like any (['foo'])", false);
        assertEvaluate("'foo' not like any ([])", false);
        assertEvaluate("'foo' not like any (['koo', 'doo'])", true);

        assertEvaluate("'foo' not ilike any ([])", false);
        assertEvaluate("'foo' not ilike any (['Koo', 'doO'])", true);
    }


    @Test
    public void testEvaluateLikeMixed() {
        assertEvaluate("'foobar' not like any (['%o_ar', '%o_a%'])", false);
        assertEvaluate("'Lorem ipsum dolor...' not like any (['%i%m%'])", false);
        assertEvaluate("'Lorem ipsum dolor...' not like any (['%%%sum%%'])", false);
        assertEvaluate("'Lorem ipsum dolor...' not like any (['%i%m'])", true);
        assertEvaluate("'fOObar' not ilike any (['%o_a%'])", false);
        assertEvaluate("'Lorem IpSuM dolor...' not ilike any (['%%%sum%%'])", false);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluateNull("null not like any ([null])");
        assertEvaluateNull("'foo' not like any ([null])");
        assertEvaluateNull("null not like any (['bar'])");
        assertEvaluateNull("null not ilike any (['bar'])");
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null not like any ([null])", isLiteral(null));
        assertNormalize("'foo' not like any ([null])", isLiteral(null));
        assertNormalize("null not like any (['bar'])", isLiteral(null));
        assertNormalize("null not ilike any (['bar'])", isLiteral(null));
    }

    @Test
    public void testNegateNotLike() throws Exception {
        assertEvaluate("not 'A' not like any (['A', 'B'])", false);
        assertEvaluate("not 'A' not ilike any (['a', 'B'])", false);
    }

    @Test
    public void test_wildcard_escaped_in_c_style_string() {
        assertEvaluate("'TextToMatch' NOT LIKE ANY ([E'Te\\%tch'])", false);
        assertEvaluate("'TextToMatch' NOT ILIKE ANY ([E'te\\%tch'])", false);
    }

    @Test
    public void test_any_not_like_ilike_with_trailing_escape_character() {
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' NOT LIKE ANY (['TextToMatch', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' NOT ILIKE ANY (['texttomatch', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
    }

    @Test
    public void test_only_rhs_arg_can_be_pattern() {
        assertEvaluate("'a%' not like any(['a__'])", true);
    }
}
