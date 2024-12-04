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
 * WARRANTIES OR CONDITIONS OF ALL KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.expression.operator.all;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class AllNotLikeOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSimpleSymbolEqual() {
        assertNormalize("'foo' not like all (['foo'])", isLiteral(false));
        assertNormalize("'notFoo' not like all (['foo'])", isLiteral(true));

        assertNormalize("'foo' not ilike all (['FoO'])", isLiteral(false));
        assertNormalize("'notFoo' not ilike all (['notFOO'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertNormalize("'%bar' not like all (['foobar', 'bar'])", isLiteral(false));
        assertNormalize("'%bar' not like all (['bar'])", isLiteral(false));
        assertNormalize("'%bar' not like all (['ar', 'car'])", isLiteral(true));
        assertNormalize("'foo%' not like all (['foobar', 'kuhbar'])", isLiteral(false));
        assertNormalize("'foo%' not like all (['foo', 'kuh'])", isLiteral(false));
        assertNormalize("'foo%' not like all (['fo', 'kuh'])", isLiteral(true));
        assertNormalize("'%oob%' not like all (['foobar'])", isLiteral(false));

        assertNormalize("'foo%' not ilike all (['FOO', 'foo'])", isLiteral(false));
        assertNormalize("'foo%' not ilike all (['fo', 'kuh'])", isLiteral(true));
        assertNormalize("'%oob%' not ilike all (['FOOBAR'])", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... all single character (exactly one)
        assertNormalize("'_ar' not like all (['bar'])", isLiteral(false));
        assertNormalize("'_bar' not like all (['bar'])", isLiteral(true));
        assertNormalize("'fo_' not like all (['foo', 'for'])", isLiteral(false));
        assertNormalize("'foo_' not like all (['foo', 'foo'])", isLiteral(true));
        assertNormalize("'_o_' not like all (['foo'])", isLiteral(false));
        assertNormalize("'_foobar_' not like all (['foobar'])", isLiteral(true));

        assertNormalize("'foo_' not ilike all (['FOO'])", isLiteral(true));
        assertNormalize("'_o_' not ilike all (['fOo'])", isLiteral(false));
        assertNormalize("'_foobar_' not ilike all (['foObar'])", isLiteral(true));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'%o_ar' not like all (['oar', 'aoar'])", isLiteral(true));
        assertNormalize("'%a_' not like all (['foobar'])", isLiteral(false));
        assertNormalize("'%o_a%' not like all (['foobar'])", isLiteral(false));
        assertNormalize("'%i%m%' not like all (['Lorem ipsum dolor...'])", isLiteral(false));
        assertNormalize("'%%%sum%%' not like all (['Lorem ipsum dolor...'])", isLiteral(false));
        assertNormalize("'%i%m' not like all (['Lorem ipsum dolor...'])", isLiteral(true));

        assertNormalize("'%i%m%' not ilike all (['LOREM IPSUM DOLOR...'])", isLiteral(false));
        assertNormalize("'%%%sum%%' not ilike all (['LOREM IPSUM DOLOR...'])", isLiteral(false));
        assertNormalize("'%i%m' not ilike all (['LOREM IPSUM DOLOR...'])", isLiteral(true));
    }

    @Test
    public void testEvaluateStraight() throws Exception {
        assertEvaluate("'foo' not like all (['joo', 'koo', 'doo'])", true);
        assertEvaluate("'foo' not like all (['foo'])", false);
        assertEvaluate("'foo' not like all ([])", true);
        assertEvaluate("'foo' not like all (['koo', 'doo'])", true);

        assertEvaluate("'foo' not ilike all ([])", true);
        assertEvaluate("'foo' not ilike all (['Koo', 'doO'])", true);
    }


    @Test
    public void testEvaluateLikeMixed() {
        assertEvaluate("'%o_ar' not like all (['foobar', 'foobaz'])", false);
        assertEvaluate("'%a_' not like all (['foobar'])", false);
        assertEvaluate("'%o_a%' not like all (['foobar'])", false);
        assertEvaluate("'%i%m%' not like all (['Lorem ipsum dolor..'])", false);
        assertEvaluate("'%%%sum%%' not like all (['Lorem ipsum dolor...'])", false);
        assertEvaluate("'%i%m' not like all (['Lorem ipsum dolor...'])", true);
        assertEvaluate("'%%%sum%%' not ilike all (['Lorem IPSUM dolor...'])", false);

    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluateNull("null not like all ([null])");
        assertEvaluateNull("'foo' not like all ([null])");
        assertEvaluateNull("null not like all (['bar'])");
        assertEvaluateNull("null not ilike all (['bar'])");
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null not like all ([null])", isLiteral(null));
        assertNormalize("'foo' not like all ([null])", isLiteral(null));
        assertNormalize("null not like all (['bar'])", isLiteral(null));
        assertNormalize("null not ilike all (['bar'])", isLiteral(null));
    }

    @Test
    public void testNegateNotLike() throws Exception {
        assertEvaluate("not 'A' not like all (['A', 'B'])", true);
        assertEvaluate("not 'A' not ilike all (['a', 'A'])", true);
    }

    @Test
    public void test_patterns_on_right_arg() {
        assertNormalize("'foobar' not like all (['%barr'])", isLiteral(true));
        assertNormalize("'bar' not like all (['_ar'])", isLiteral(false));
        assertNormalize("'foobar' not like all (['%o_a%'])", isLiteral(false));
        assertNormalize("'fOobAR' not ilike all (['%BaR'])", isLiteral(false));
        assertNormalize("'BaR' not ilike all (['z_ar'])", isLiteral(true));
        assertNormalize("'foobar' not ilike all (['%O_a%'])", isLiteral(false));
    }

    @Test
    public void test_wildcard_escaped_in_c_style_string() {
        assertEvaluate("'TextToMatch' NOT LIKE ALL ([E'Te\\%tch'])", false);
        assertEvaluate("'TextToMatch' NOT ILIKE ALL ([E'te\\%tch'])", false);
    }

    @Test
    public void test_all_not_like_ilike_with_trailing_escape_character() {
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' NOT LIKE ALL (['TextToMatch', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' NOT ILIKE ALL (['texttomatch', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
    }
}
