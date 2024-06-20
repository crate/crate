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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class ArithmeticOverflowTest extends ScalarTestCase {

    @Test
    public void test_integer_overflow() {
        assertThatThrownBy(() -> assertEvaluateNull("2147483647::integer + 1::integer"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void test_integer_overflow_from_table() {
        assertThatThrownBy(() -> assertEvaluateNull("a + 1::integer", Literal.of(Integer.MAX_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void test_integer_overflow_mul() {
        assertThatThrownBy(() -> assertEvaluateNull("2147483647::integer * 2::integer"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void test_integer_overflow_mul_from_table() {
        assertThatThrownBy(() -> assertEvaluateNull("a * 2::integer", Literal.of(Integer.MAX_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void test_integer_underflow() {
        assertThatThrownBy(() -> assertEvaluateNull("-2147483647::integer - 2::integer"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void test_integer_underflow_from_table() {
        assertThatThrownBy(() -> assertEvaluateNull("a - 1::integer", Literal.of(Integer.MIN_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void test_long_overflow() {
        assertThatThrownBy(() -> assertEvaluateNull("9223372036854775807 + 1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("long overflow");
    }

    @Test
    public void test_long_overflow_from_table() {
        assertThatThrownBy(() -> assertEvaluateNull("x + 1", Literal.of(Long.MAX_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("long overflow");
    }

    @Test
    public void test_long_overflow_mul() {
        assertThatThrownBy(() -> assertEvaluateNull("9223372036854775807 * 2"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("long overflow");
    }

    @Test
    public void test_long_overflow_mul_from_table() {
        assertThatThrownBy(() -> assertEvaluateNull("x * 2", Literal.of(Long.MAX_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("long overflow");
    }

    @Test
    public void test_long_underflow() {
        assertThatThrownBy(() -> assertEvaluateNull("-9223372036854775807 - 2"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("long overflow");
    }

    @Test
    public void test_long_underflow_from_table() {
        assertThatThrownBy(() -> assertEvaluateNull("x - 1", Literal.of(Long.MIN_VALUE)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("long overflow");
    }
}
