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
package io.crate.operator.operator;

import com.google.common.collect.ImmutableList;
import io.crate.operator.operator.input.BytesRefInput;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.junit.Test;

import static io.crate.operator.operator.LikeOperator.DEFAULT_ESCAPE;
import static io.crate.operator.operator.LikeOperator.expressionToRegex;
import static org.junit.Assert.*;

public class LikeOperatorTest {

    private static Symbol normalizeSymbol(String expression, String pattern) {
        LikeOperator op = new LikeOperator(
                LikeOperator.generateInfo(LikeOperator.NAME, DataType.STRING)
        );
        Function function = new Function(
                op.info(), 
                ImmutableList.<Symbol>of(new StringLiteral(expression), new StringLiteral(pattern))
        );
        return op.normalizeSymbol(function);
    }

    private Boolean likeNormalize(String expression, String pattern) {
        return ((BooleanLiteral) normalizeSymbol(expression, pattern)).value();
    }

    @Test
    public void testNormalizeSymbolEqual() {
        assertTrue(likeNormalize("foo", "foo"));
        assertFalse(likeNormalize("foo", "notFoo"));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)
        assertTrue(likeNormalize("%bar", "foobar"));
        assertTrue(likeNormalize("%bar", "bar"));
        assertFalse(likeNormalize("%bar", "ar"));
        assertTrue(likeNormalize("foo%", "foobar"));
        assertTrue(likeNormalize("foo%", "foo"));
        assertFalse(likeNormalize("foo%", "fo"));
        assertTrue(likeNormalize("%oob%", "foobar"));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertTrue(likeNormalize("_ar", "bar"));
        assertFalse(likeNormalize("_bar", "bar"));
        assertTrue(likeNormalize("fo_", "foo"));
        assertFalse(likeNormalize("foo_", "foo"));
        assertTrue(likeNormalize("_o_", "foo"));
        assertFalse(likeNormalize("_foobar_", "foobar"));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertTrue(likeNormalize("%o_ar", "foobar"));
        assertTrue(likeNormalize("%a_", "foobar"));
        assertTrue(likeNormalize("%o_a%", "foobar"));
        assertTrue(likeNormalize("%i%m%", "Lorem ipsum dolor..."));
        assertTrue(likeNormalize("%%%sum%%", "Lorem ipsum dolor..."));
        assertFalse(likeNormalize("%i%m", "Lorem ipsum dolor..."));
    }

    // Following tests: escaping wildcards

    @Test
    public void testExpressionToRegexExactlyOne() {
        String expression = "fo_bar";
        assertEquals("^fo.bar$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexZeroOrMore() {
        String expression = "fo%bar";
        assertEquals("^fo.*bar$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingPercent() {
        String expression = "fo\\%bar";
        assertEquals("^fo%bar$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingUnderline() {
        String expression = "fo\\_bar";
        assertEquals("^fo_bar$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscaping() {
        String expression = "fo\\\\_bar";
        assertEquals("^fo\\\\.bar$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingMutli() {
        String expression = "%%\\%sum%%";
        assertEquals("^.*.*%sum.*.*$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexMaliciousPatterns() {
        String expression = "fo(ooo)o[asdf]o\\bar^$.*";
        assertEquals("^fo\\(ooo\\)o\\[asdf\\]obar\\^\\$\\.\\*$", expressionToRegex(expression, DEFAULT_ESCAPE, true));
    }

    // test evaluate

    private Boolean like(String expression, String pattern) {
        LikeOperator op = new LikeOperator(
                LikeOperator.generateInfo(LikeOperator.NAME, DataType.STRING)
        );
        return op.evaluate(new BytesRefInput(expression),new BytesRefInput(pattern));
    }

    @Test
    public void testLikeOperator() {
        assertTrue(like("foo%baz", "foobarbaz"));
        assertFalse(like("foo_baz", "foobarbaz"));

        // set the Input.value() to null.
        LikeOperator op = new LikeOperator(
                LikeOperator.generateInfo(LikeOperator.NAME, DataType.STRING)
        );
        BytesRef nullValue = null;
        BytesRefInput brNullValue = new BytesRefInput(nullValue);
        assertNull(op.evaluate(brNullValue, new BytesRefInput("foobarbaz")));
        assertNull(op.evaluate(new BytesRefInput("foobarbaz"), brNullValue));
    }

}
