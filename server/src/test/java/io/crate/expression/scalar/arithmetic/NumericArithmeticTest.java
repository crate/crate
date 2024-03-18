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

import java.math.BigDecimal;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class NumericArithmeticTest extends ScalarTestCase {

    @Test
    public void test_numeric_add() {
        assertEvaluate("12.123::numeric(4, 2) + 10.14::numeric",
                       new BigDecimal("22.26"));
    }

    @Test
    public void test_numeric_subtract() {
        assertEvaluate("12.12::numeric(4, 2) - 10.14::numeric(3)",
                       new BigDecimal("2.12"));
    }

    @Test
    public void test_numeric_multiply() {
        assertEvaluate("12.12::numeric * 10.14::numeric",
                       new BigDecimal("122.8968"));
    }

    @Test
    public void test_numeric_divide() {
        assertEvaluate("12.12::numeric(4, 2) / 10.14::numeric(4, 2)",
                       new BigDecimal("1.195266272189349"));
    }

    @Test
    public void test_numeric_modulus() {
        assertEvaluate("12.12::numeric(4, 2) % 10.14::numeric(4, 2)",
                       new BigDecimal("1.98"));
    }

    @Test
    public void test_numeric_exp() {
        assertEvaluate("3::numeric ^ 4::numeric" ,
            new BigDecimal("81.0").doubleValue());
    }

    @Test
    public void test_numeric_exp_mixed_operators() {
        assertEvaluate(
            "3::numeric * 4::numeric + 4::numeric + 5::numeric * 5::numeric ^ 7::numeric",
            new BigDecimal("390641.0")
        );
    }



}
