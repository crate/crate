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

public class AllLikeOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSimpleSymbolEqual() {
        assertNormalize("'foo' like all (['foo'])", isLiteral(true));
        assertNormalize("'notFoo' like all (['foo', 'notFoo'])", isLiteral(false));
        assertNormalize("'foo' ilike all (['fOo', 'FOO'])", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertNormalize("'bar' like all (['%bar'])", isLiteral(true));
        assertNormalize("'foobar' like all (['%bar'])", isLiteral(true));
        assertNormalize("'bar' like all (['%ar', '%car'])", isLiteral(false));
        assertNormalize("'bar' like all (['%car', '%dar'])", isLiteral(false));
        assertNormalize("'foobar' like all (['%oob%'])", isLiteral(true));
        assertNormalize("'FOObAr' ilike all (['%bar'])", isLiteral(true));
        assertNormalize("'bOOb' ilike all (['%oob%'])", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... all single character (exactly one)
        assertNormalize("'bar' like all (['_ar'])", isLiteral(true));
        assertNormalize("'bar' like all (['_bar'])", isLiteral(false));
        assertNormalize("'foot' like all (['foo_'])", isLiteral(true));
        assertNormalize("'foo' like all (['foo_'])", isLiteral(false));
        assertNormalize("'foo' like all (['_o_'])", isLiteral(true));
        assertNormalize("'foobar' like all (['_foobar_'])", isLiteral(false));
        assertNormalize("'bAR' ilike all (['_ar'])", isLiteral(true));
        assertNormalize("'fOo' ilike all (['_o_'])", isLiteral(true));
        assertNormalize("'foo' ilike all (['fOO_'])", isLiteral(false));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'foobar' like all (['%o_ar'])", isLiteral(true));
        assertNormalize("'foobar' like all (['%o_a%'])", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like all (['%i%m%'])", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like all (['%%%sum%%'])", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like all (['%i%m'])", isLiteral(false));
        assertNormalize("'fOObar' ilike all (['%o_a%'])", isLiteral(true));
        assertNormalize("'Lorem IpSuM dolor...' ilike all (['%%%sum%%'])", isLiteral(true));
    }

    @Test
    public void testEvaluateStraight() throws Exception {
        assertEvaluate("'foo' like all (['foo', 'foo', 'foo'])", true);
        assertEvaluate("'foo' like all (['foo'])", true);
        assertEvaluate("'foo' like all ([])", true);
        assertEvaluate("'foo' like all (['foo', 'koo', 'doo'])", false);
    }

    @Test
    public void testEvaluateLikeMixed() {
        assertEvaluate("'foobar' like all (['%o_ar'])", true);
        assertEvaluate("'foobar' like all (['%o_a%'])", true);
        assertEvaluate("'Lorem ipsum dolor...' like all (['%i%m%'])", true);
        assertEvaluate("'Lorem ipsum dolor...' like all (['%%%sum%%'])", true);
        assertEvaluate("'Lorem ipsum dolor...' like all (['%i%m'])", false);
        assertEvaluate("'fOObar' ilike all (['%o_a%'])", true);
        assertEvaluate("'Lorem IpSuM dolor...' ilike all (['%%%sum%%'])", true);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluateNull("null like all([null])");
        assertEvaluateNull("'foo'like all([null])");
        assertEvaluateNull("null like all(['bar'])");
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null like all([null])", isLiteral(null));
        assertNormalize("'foo'like all([null])", isLiteral(null));
        assertNormalize("null like all(['bar'])", isLiteral(null));

        assertNormalize("null ilike all([null])", isLiteral(null));
        assertNormalize("'foo'ilike all([null])", isLiteral(null));
        assertNormalize("null ilike all(['bar'])", isLiteral(null));
    }

    @Test
    public void testNegateLike() throws Exception {
        assertEvaluate("not 'A' like all (['A'])", false);
        assertEvaluate("not 'A' ilike all (['a'])", false);
    }

    @Test
    public void test_non_string_values() {
        assertNormalize("1 like all ([1, null, 1])", isLiteral(null));
        assertNormalize("1 not like all ([1])", isLiteral(false));
        assertNormalize("1 ilike all ([1, 1])", isLiteral(true));
        assertNormalize("1 not ilike all ([1])", isLiteral(false));
    }

    @Test
    public void test_wildcard_escaped_in_c_style_string() {
        assertEvaluate("'TextToMatch' LIKE ALL ([E'Te\\%tch'])", true);
        assertEvaluate("'TextToMatch' ILIKE ALL ([E'te\\%tch'])", true);
    }

    @Test
    public void test_all_like_ilike_with_trailing_escape_character() {
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' LIKE ALL (['a', 'b', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
        assertThatThrownBy(() -> assertEvaluate("'TextToMatch' ILIKE ALL (['a', 'b', 'ab\\'])", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("pattern 'ab\\' must not end with escape character '\\'");
    }

    @Test
    public void test_only_rhs_arg_can_be_pattern() {
        assertEvaluate("'a%' like all(['a__'])", false);
    }
}
