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

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class ArithmeticOverflowTest extends ScalarTestCase {

    @Test
    public void test_integer_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluateNull("2147483647::integer + 1::integer");
    }

    @Test
    public void test_integer_overflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluateNull("a + 1::integer", Literal.of(Integer.MAX_VALUE));
    }

    @Test
    public void test_integer_overflow_mul() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluateNull("2147483647::integer * 2::integer");
    }

    @Test
    public void test_integer_overflow_mul_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluateNull("a * 2::integer", Literal.of(Integer.MAX_VALUE));
    }

    @Test
    public void test_integer_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluateNull("-2147483647::integer - 2::integer");
    }

    @Test
    public void test_integer_underflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluateNull("a - 1::integer", Literal.of(Integer.MIN_VALUE));
    }

    @Test
    public void test_long_overflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluateNull("9223372036854775807 + 1");
    }

    @Test
    public void test_long_overflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluateNull("x + 1", Literal.of(Long.MAX_VALUE));
    }

    @Test
    public void test_long_overflow_mul() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluateNull("9223372036854775807 * 2");
    }

    @Test
    public void test_long_overflow_mul_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluateNull("x * 2", Literal.of(Long.MAX_VALUE));
    }

    @Test
    public void test_long_underflow() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluateNull("-9223372036854775807 - 2");
    }

    @Test
    public void test_long_underflow_from_table() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("long overflow");
        assertEvaluateNull("x - 1", Literal.of(Long.MIN_VALUE));
    }
}
