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

import static io.crate.expression.operator.LikeOperators.DEFAULT_ESCAPE;
import static io.crate.expression.operator.LikeOperators.patternToRegex;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class LikeOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbolEqual() {
        assertNormalize("'foo' like 'foo'", isLiteral(true));
        assertNormalize("'notFoo' like 'foo'", isLiteral(false));

        assertNormalize("'foo' ilike 'FOO'", isLiteral(true));
        assertNormalize("'FOO' ilike 'foo'", isLiteral(true));
        assertNormalize("'FOO' ilike 'FOO'", isLiteral(true));
    }

    @Test
    public void testPatternIsNoLiteral() throws Exception {
        assertEvaluate("name like timezone", false, Literal.of("foo"), Literal.of("bar"));
        assertEvaluate("name like name", true, Literal.of("foo"), Literal.of("foo"));

        assertEvaluate("name ilike timezone", false, Literal.of("foO"), Literal.of("FFo"));
        assertEvaluate("name ilike timezone", true, Literal.of("foO"), Literal.of("FOo"));
        assertEvaluate("name ilike timezone", true, Literal.of("foO"), Literal.of("F__"));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)

        assertNormalize("'foobar' like '%bar'", isLiteral(true));
        assertNormalize("'bar' like '%bar'", isLiteral(true));
        assertNormalize("'ar' like '%bar'", isLiteral(false));
        assertNormalize("'foobar' like 'foo%'", isLiteral(true));
        assertNormalize("'foo' like 'foo%'", isLiteral(true));
        assertNormalize("'fo' like 'foo%'", isLiteral(false));
        assertNormalize("'foobar' like '%oob%'", isLiteral(true));

        assertNormalize("'fOobAr' ilike '%BaR'", isLiteral(true));
        assertNormalize("'Fo' ilike 'fOo%'", isLiteral(false));
        assertNormalize("'foobar' ilike '%OoB%'", isLiteral(true));

    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertNormalize("'bar' like '_ar'", isLiteral(true));
        assertNormalize("'bar' like '_bar'", isLiteral(false));
        assertNormalize("'foo' like 'fo_'", isLiteral(true));
        assertNormalize("'foo' like 'foo_'", isLiteral(false));
        assertNormalize("'foo' like '_o_'", isLiteral(true));
        assertNormalize("'foo' like '_o_'", isLiteral(true));

        assertNormalize("'foObAr' ilike '_OoBa_'", isLiteral(true));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'foobar' like '%o_ar'", isLiteral(true));
        assertNormalize("'foobar' like '%a_'", isLiteral(true));
        assertNormalize("'foobar' like '%o_a%'", isLiteral(true));

        assertNormalize("'Lorem ipsum dolor...' like '%i%m%'", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like '%%%sum%%'", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like '%i%m'", isLiteral(false));

        assertNormalize("'Lorem IPSUM dolor...' ilike '%i%m%'", isLiteral(true));

    }

    // Following tests: escaping wildcards

    @Test
    public void testExpressionToRegexExactlyOne() {
        String expression = "fo_bar";
        assertEquals("^fo.bar$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testLikeOnMultilineStatement() throws Exception {
        String stmt = "SELECT date_trunc('day', ts), sum(num_steps) as num_steps, count(*) as num_records \n" +
                      "FROM steps\n" +
                      "WHERE month_partition = '201409'\n" +
                      "GROUP BY 1 ORDER BY 1 DESC limit 100";

        assertEvaluate("name like '  SELECT%'", false, Literal.of(stmt));
        assertEvaluate("name like 'SELECT%'", true, Literal.of(stmt));
        assertEvaluate("name like 'SELECT date_trunc%'", true, Literal.of(stmt));
        assertEvaluate("name like '% date_trunc%'", true, Literal.of(stmt));
    }

    @Test
    public void testExpressionToRegexZeroOrMore() {
        String expression = "fo%bar";
        assertEquals("^fo.*bar$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testExpressionToRegexEscapingPercent() {
        String expression = "fo\\%bar";
        assertEquals("^fo%bar$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testExpressionToRegexEscapingUnderline() {
        String expression = "fo\\_bar";
        assertEquals("^fo_bar$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testExpressionToRegexEscaping() {
        String expression = "fo\\\\_bar";
        assertEquals("^fo\\\\.bar$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testExpressionToRegexEscapingMutli() {
        String expression = "%%\\%sum%%";
        assertEquals("^.*.*%sum.*.*$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testExpressionToRegexMaliciousPatterns() {
        String expression = "fo(ooo)?o[asdf]o\\bar^$.*";
        assertEquals("^fo\\(ooo\\)\\?o\\[asdf\\]obar\\^\\$\\.\\*$", patternToRegex(expression, DEFAULT_ESCAPE));
    }

    @Test
    public void testLikeOperator() {
        assertEvaluate("'foobarbaz' like 'foo%baz'", true);
        assertEvaluate("'foobarbaz' like 'foo_baz'", false);
        assertEvaluate("'characters' like 'charac%'", true);
        assertEvaluate("'my.domain.com?path' like '%com?path%'", true);

        assertEvaluateNull("'foobarbaz' like name", Literal.NULL);
        assertEvaluateNull("name like 'foobarbaz'", Literal.NULL);
    }

    @Test
    public void testIlikeOperator() {
        assertEvaluate("'FOOBARBAZ' ilike 'foo%baz'", true);
        assertEvaluate("'FOOBARBAZ' ilike 'foo___baz'", true);
        assertEvaluate("'characters' ilike 'CHaraC%'", true);
        assertEvaluate("'my.domain.com?path' ilike '%com?pATh%'", true);

        assertEvaluateNull("'foobarbaz' ilike name", Literal.NULL);
        assertEvaluateNull("name ilike 'foobarbaz'", Literal.NULL);
    }

    @Test
    public void testPatternToRegexPrependsBackSlashBeforeCurlyBraces() {
        String expression = "{}";
        var re = patternToRegex(expression, DEFAULT_ESCAPE);
        assertEquals("^\\{\\}$", re);
    }

    @Test
    public void test_wildcard_escaped_in_c_style_string() {
        assertEvaluate("'TextToMatch' LIKE E'Te\\%tch'", true);
        assertEvaluate("'TextToMatch' NOT LIKE E'Te\\%tch'", false);
        assertEvaluate("'TextToMatch' ILIKE E'te\\%tch'", true);
        assertEvaluate("'TextToMatch' NOT ILIKE E'te\\%tch'", false);
    }

    @Test
    public void test_custom_escape_character() {
        assertEvaluate("'Test' LIKE 'Te%' escape 'e'", false); // % is taken literally
        assertEvaluate("'T%' LIKE 'Te%' escape 'e'", true); // % is taken literally
        // Negate expressions above
        assertEvaluate("'Test' NOT LIKE 'Te%' escape 'e'", true);
        assertEvaluate("'T%' NOT LIKE 'Te%' escape 'e'", false);

        // Case-insensitive matching, pattern has only lowercase characters.
        assertEvaluate("'Test' ILIKE 'te%' escape 'e'", false); // % is taken literally
        assertEvaluate("'T%' ILIKE 'te%' escape 'e'", true); // % is taken literally
        // Negate expressions above
        assertEvaluate("'Test' NOT ILIKE 'te%' escape 'e'", true);
        assertEvaluate("'T%' NOT ILIKE 'te%' escape 'e'", false);

    }

    @Test
    public void test_like_with_non_single_char_escape_throws_error() {
        assertThatThrownBy(() -> assertEvaluate("'Test' LIKE 'Te%' ESCAPE 'ab'", false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("ESCAPE must be a single character");
    }

    @Test
    public void test_like_with_empty_escape_disables_escaping() {
        assertEvaluate("'Test' LIKE 'T\\%' ESCAPE ''", false);
    }
}
