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
    public void testNormalizeSingleSymbolEqual() {
        assertNormalize("'foo' not like any (['foo'])", isLiteral(false));
        assertNormalize("'notFoo' not like any (['foo'])", isLiteral(true));

        assertNormalize("'foo' not ilike any (['FoO'])", isLiteral(false));
        assertNormalize("'notFoo' not ilike any (['notFOO'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertNormalize("'%bar' not like any (['foobar', 'bar'])", isLiteral(false));
        assertNormalize("'%bar' not like any (['bar'])", isLiteral(false));
        assertNormalize("'%bar' not like any (['ar', 'car'])", isLiteral(true));
        assertNormalize("'foo%' not like any (['foobar', 'kuhbar'])", isLiteral(true));
        assertNormalize("'foo%' not like any (['foo', 'kuh'])", isLiteral(true));
        assertNormalize("'foo%' not like any (['fo', 'kuh'])", isLiteral(true));
        assertNormalize("'%oob%' not like any (['foobar'])", isLiteral(false));

        assertNormalize("'foo%' not ilike any (['FOO', 'kuh'])", isLiteral(true));
        assertNormalize("'foo%' not ilike any (['fo', 'kuh'])", isLiteral(true));
        assertNormalize("'%oob%' not ilike any (['FOOBAR'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertNormalize("'_ar' not like any (['bar'])", isLiteral(false));
        assertNormalize("'_bar' not like any (['bar'])", isLiteral(true));
        assertNormalize("'fo_' not like any (['foo', 'for'])", isLiteral(false));
        assertNormalize("'foo_' not like any (['foo', 'foot'])", isLiteral(true));
        assertNormalize("'foo_' not like any (['foo'])", isLiteral(true));
        assertNormalize("'_o_' not like any (['foo'])", isLiteral(false));
        assertNormalize("'_foobar_' not like any (['foobar'])", isLiteral(true));

        assertNormalize("'foo_' not ilike any (['FOO'])", isLiteral(true));
        assertNormalize("'_o_' not ilike any (['fOo'])", isLiteral(false));
        assertNormalize("'_foobar_' not ilike any (['foObar'])", isLiteral(true));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'%o_ar' not like any (['foobar', 'foobaz'])", isLiteral(true));
        assertNormalize("'%a_' not like any (['foobar'])", isLiteral(false));
        assertNormalize("'%o_a%' not like any (['foobar'])", isLiteral(false));
        assertNormalize("'%i%m%' not like any (['Lorem ipsum dolor...'])", isLiteral(false));
        assertNormalize("'%%%sum%%' not like any (['Lorem ipsum dolor...'])", isLiteral(false));
        assertNormalize("'%i%m' not like any (['Lorem ipsum dolor...'])", isLiteral(true));

        assertNormalize("'%i%m%' not ilike any (['LOREM IPSUM DOLOR...'])", isLiteral(false));
        assertNormalize("'%%%sum%%' not ilike any (['LOREM IPSUM DOLOR...'])", isLiteral(false));
        assertNormalize("'%i%m' not ilike any (['LOREM IPSUM DOLOR...'])", isLiteral(true));
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
        assertEvaluate("'%o_ar' not like any (['foobar', 'foobaz'])", true);
        assertEvaluate("'%a_' not like any (['foobar'])", false);
        assertEvaluate("'%o_a%' not like any (['foobar'])", false);
        assertEvaluate("'%i%m%' not like any (['Lorem ipsum dolor..'])", false);
        assertEvaluate("'%%%sum%%' not like any (['Lorem ipsum dolor...'])", false);
        assertEvaluate("'%i%m' not like any (['Lorem ipsum dolor...'])", true);
        assertEvaluate("'%%%sum%%' not ilike any (['Lorem IPSUM dolor...'])", false);

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
    public void test_patterns_on_right_arg() {
        assertNormalize("'foobar' not like any (['%barr'])", isLiteral(true));
        assertNormalize("'bar' not like any (['_ar'])", isLiteral(false));
        assertNormalize("'foobar' not like any (['%o_a%'])", isLiteral(false));
        assertNormalize("'fOobAR' not ilike any (['%BaR'])", isLiteral(false));
        assertNormalize("'BaR' not ilike any (['z_ar'])", isLiteral(true));
        assertNormalize("'foobar' not ilike any (['%O_a%'])", isLiteral(false));
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
}
