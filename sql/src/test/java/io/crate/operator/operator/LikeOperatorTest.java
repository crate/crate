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
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
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

    @Test
    public void testNormalizeSymbolEqual() {
        Symbol result = normalizeSymbol("foo", "foo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolNotEqual() {
        Symbol result = normalizeSymbol("foo", "notFoo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral) result).value());
    }

    // Following tests: wildcard: '%' ... zero or more characters (0...N)

    @Test
    public void testNormalizeSymbolLikeZeroOrMoreLeftN() {
        Symbol result = normalizeSymbol("%bar", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMoreLeftZero() {
        Symbol result = normalizeSymbol("%bar", "bar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolNotLikeZeroOrMoreLeft() {
        Symbol result = normalizeSymbol("%bar", "ar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral) result).value());
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMoreRightN() {
        Symbol result = normalizeSymbol("foo%", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMoreRightZero() {
        Symbol result = normalizeSymbol("foo%", "foo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolNotLikeZeroOrMoreRight() {
        Symbol result = normalizeSymbol("foo%", "fo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral) result).value());
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMoreLeftRightN() {
        Symbol result = normalizeSymbol("%oob%", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    // Following tests: wildcard: '_' ... any single character (exactly one)

    @Test
    public void testNormalizeSymbolLikeSingleLeft() {
        Symbol result = normalizeSymbol("_ar", "bar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolNotLikeSingleLeft() {
        Symbol result = normalizeSymbol("_bar", "bar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral) result).value());
    }

    @Test
    public void testNormalizeSymbolLikeSingleRight() {
        Symbol result = normalizeSymbol("fo_", "foo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolNotLikeSingleRight() {
        Symbol result = normalizeSymbol("foo_", "foo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral) result).value());
    }

    @Test
    public void testNormalizeSymbolLikeSingleLeftRight() {
        Symbol result = normalizeSymbol("_o_", "foo");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral) result).value());
    }

    @Test
    public void testNormalizeSymbolNotLikeSingleLeftRight() {
        Symbol result = normalizeSymbol("_foobar_", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral) result).value());
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        Symbol result = normalizeSymbol("%o_ar", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolLikeMixed2() {
        Symbol result = normalizeSymbol("%a_", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolLikeMixedMiddle() {
        Symbol result = normalizeSymbol("%o_a%", "foobar");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolLikeMixedMiddle2() {
        Symbol result = normalizeSymbol("%i%m%", "Lorem ipsum dolor...");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertTrue(((BooleanLiteral)result).value());
    }

    @Test
    public void testNormalizeSymbolNotLikeMixedMiddle() {
        Symbol result = normalizeSymbol("%i%m", "Lorem ipsum dolor...");
        assertThat(result, instanceOf(BooleanLiteral.class));
        assertFalse(((BooleanLiteral)result).value());
    }
}
