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

public class PowerFunctionTest extends ScalarTestCase {

    @Test
    public void testPowerWithIntegers() {
        assertEvaluate("power(2,4)", 16.0);
    }

    @Test
    public void testPowerWithDecimalTypes() {
        assertEvaluate("power(2.0,4.0)", 16.0);
    }

    @Test
    public void testNegativeExponent() {
        assertEvaluate("power(2,-3)", 0.125);
    }

    @Test
    public void testNegativeDecimalTypeExponent() {
        assertEvaluate("power(2,-3.0)", 0.125);
    }

    @Test
    public void testNegativeBaseWithPositiveExponent() {
        assertEvaluate("power(-2,3)", -8.0);
    }

    @Test
    public void testNegativeBaseAndExponent() {
        assertEvaluate("power(-2,-3)", -0.125);
    }

    @Test
    public void testNegativeBaseAndExponentDecimalType() {
        assertEvaluate("power(-2.0,-3.0)", -0.125);
    }

    @Test
    public void testInvalidNumberOfArguments() {
        assertThatThrownBy(() -> assertEvaluateNull("power(2)"))
            .hasMessageStartingWith("Unknown function: power(2)," +
                    " no overload found for matching argument types: (integer).");
    }
}
