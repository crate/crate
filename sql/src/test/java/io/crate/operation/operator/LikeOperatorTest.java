/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operation.operator;

import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.operation.operator.LikeOperator.DEFAULT_ESCAPE;

public class LikeOperatorTest extends CrateUnitTest {

    private static Symbol normalizeSymbol(String expression, String pattern) {
        LikeOperator op = new LikeOperator(
                LikeOperator.generateInfo(LikeOperator.NAME, DataTypes.STRING)
        );
        Function function = new Function(
                op.info(), 
                Arrays.<Symbol>asList(Literal.newLiteral(expression), Literal.newLiteral(pattern))
        );
        return op.normalizeSymbol(function);
    }

    private Boolean likeNormalize(String expression, String pattern) {
        return (Boolean)((Literal)normalizeSymbol(expression, pattern)).value();
    }

    @Test
    public void testNormalizeSymbolEqual() {
        assertTrue(likeNormalize("foo", "foo"));
        assertFalse(likeNormalize("notFoo", "foo"));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertTrue(likeNormalize("foobar", "%bar"));
        assertTrue(likeNormalize("bar", "%bar"));
        assertFalse(likeNormalize("ar", "%bar"));
        assertTrue(likeNormalize("foobar", "foo%"));
        assertTrue(likeNormalize("foo", "foo%"));
        assertFalse(likeNormalize("fo", "foo%"));
        assertTrue(likeNormalize("foobar", "%oob%"));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertTrue(likeNormalize("bar", "_ar"));
        assertFalse(likeNormalize("bar", "_bar"));
        assertTrue(likeNormalize("foo", "fo_"));
        assertFalse(likeNormalize("foo", "foo_"));
        assertTrue(likeNormalize("foo", "_o_"));
        assertFalse(likeNormalize("foobar", "_foobar_"));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertTrue(likeNormalize("foobar", "%o_ar"));
        assertTrue(likeNormalize("foobar", "%a_"));
        assertTrue(likeNormalize("foobar", "%o_a%"));
        assertTrue(likeNormalize("Lorem ipsum dolor...", "%i%m%"));
        assertTrue(likeNormalize("Lorem ipsum dolor...", "%%%sum%%"));
        assertFalse(likeNormalize("Lorem ipsum dolor...", "%i%m"));
    }

    // Following tests: escaping wildcards

    @Test
    public void testExpressionToRegexExactlyOne() {
        String expression = "fo_bar";
        assertEquals("^fo.bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testLikeOnMultilineStatement() throws Exception {
        String stmt = "SELECT date_trunc('day', ts), sum(num_steps) as num_steps, count(*) as num_records \n" +
                "FROM steps\n" +
                "WHERE month_partition = '201409'\n" +
                "GROUP BY 1 ORDER BY 1 DESC limit 100";

        assertFalse(likeNormalize(stmt, "  SELECT%"));
        assertTrue(likeNormalize(stmt, "SELECT%"));
        assertTrue(likeNormalize(stmt, "SELECT date_trunc%"));
        assertTrue(likeNormalize(stmt, "% date_trunc%"));
    }

    @Test
    public void testExpressionToRegexZeroOrMore() {
        String expression = "fo%bar";
        assertEquals("^fo.*bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingPercent() {
        String expression = "fo\\%bar";
        assertEquals("^fo%bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingUnderline() {
        String expression = "fo\\_bar";
        assertEquals("^fo_bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscaping() {
        String expression = "fo\\\\_bar";
        assertEquals("^fo\\\\.bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingMutli() {
        String expression = "%%\\%sum%%";
        assertEquals("^.*.*%sum.*.*$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexMaliciousPatterns() {
        String expression = "fo(ooo)o[asdf]o\\bar^$.*";
        assertEquals("^fo\\(ooo\\)o\\[asdf\\]obar\\^\\$\\.\\*$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    // test evaluate

    private Boolean like(String expression, String pattern) {
        LikeOperator op = new LikeOperator(
                LikeOperator.generateInfo(LikeOperator.NAME, DataTypes.STRING)
        );
        return op.evaluate(Literal.newLiteral(expression), Literal.newLiteral(pattern));
    }

    @Test
    public void testLikeOperator() {
        assertTrue(like("foobarbaz", "foo%baz"));
        assertFalse(like("foobarbaz", "foo_baz"));
        assertTrue(like("characters", "charac%"));

        // set the Input.value() to null.
        LikeOperator op = new LikeOperator(
                LikeOperator.generateInfo(LikeOperator.NAME, DataTypes.STRING)
        );
        BytesRef nullValue = null;
        Literal<BytesRef> brNullValue = Literal.newLiteral(nullValue);
        assertNull(op.evaluate(brNullValue, Literal.newLiteral("foobarbaz")));
        assertNull(op.evaluate(Literal.newLiteral("foobarbaz"), brNullValue));
    }

}
