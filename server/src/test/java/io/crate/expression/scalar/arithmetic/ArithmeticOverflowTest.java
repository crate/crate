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

package io.crate.expression.scalar.arithmetic;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

public class ArithmeticOverflowTest extends ScalarTestCase {

    @Test
    public void test_integer_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("2147483647::integer + 1::integer", null);
    }

    @Test
    public void test_integer_overflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("a + 1::integer", null, Literal.of(Integer.MAX_VALUE));
    }

    @Test
    public void test_integer_overflow_mul() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("2147483647::integer * 2::integer", null);
    }

    @Test
    public void test_integer_overflow_mul_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("a * 2::integer", null, Literal.of(Integer.MAX_VALUE));
    }

    @Test
    public void test_integer_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("-2147483647::integer - 2::integer", null);
    }

    @Test
    public void test_integer_underflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("a - 1::integer", null, Literal.of(Integer.MIN_VALUE));
    }

    @Test
    public void test_long_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluate("9223372036854775807 + 1", null);
    }

    @Test
    public void test_long_overflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluate("x + 1", null, Literal.of(Long.MAX_VALUE));
    }

    @Test
    public void test_long_overflow_mul() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluate("9223372036854775807 * 2", null);
    }

    @Test
    public void test_long_overflow_mul_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluate("x * 2", null, Literal.of(Long.MAX_VALUE));
    }

    @Test
    public void test_long_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluate("-9223372036854775807 - 2", null);
    }

    @Test
    public void test_long_underflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluate("x - 1", null, Literal.of(Long.MIN_VALUE));
    }
}
