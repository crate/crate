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

package io.crate.expression.scalar;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;

public class ArrayUpperFunctionTest extends ScalarTestCase {

    @Test
    public void testSecondArgumentIsNull() {
        assertEvaluateNull("array_upper([1, 2], null)");
    }

    @Test
    public void test_not_null_but_invalid_dimension() {
        assertEvaluateNull("array_length([1], 0)");
        assertEvaluateNull("array_length([1], -1)");
    }

    @Test
    public void test_3d_array_with_mixed_sizes_within_dimension() {
        // arr[3][2-3][2-3]
        // This test shows that we have to traverse all arrays on each dimension.
        // Second 2D has only 2 rows, but long and third 2D arrays consists of more arrays but they are shorter and this affects
        // Also testing null on each dimension.
        String array =
            "[null," +           // first 2D array is null
            "[null, [null, 1, 1]], " + // second 2D array's first row is null
            "[[1, 1], [1, 1], [1, 1]]]";

        assertEvaluate("array_length(" + array + ", 1)", 3);
        assertEvaluate("array_length(" + array + ", 2)", 3);
        assertEvaluate("array_length(" + array + ", 3)", 3);
        assertEvaluateNull("array_length(" + array + ", 4)");
    }

    @Test
    public void testSingleDimensionArrayValidDimension() {
        assertEvaluate("array_upper([4, 5], 1)", 2);
    }

    @Test
    public void testSingleDimensionArrayInvalidDimension() {
        assertEvaluateNull("array_upper([4, 5], 3)");
    }

    @Test
    public void testMultiDimensionArrayValidDimension() {
        // CrateDB allows usage of arrays with different sizes on the same dimension.
        // Array upper returns maximal size on the given dimension.
        assertEvaluate("array_upper([[3, 4], [1, 2, 3]], 2)", 3);
    }

    @Test
    public void testMultiDimensionArrayInvalidDimension() {
        assertEvaluateNull("array_upper([[1, 2, 3], [3, 4]], 3)");
    }

    @Test
    public void testCanBeAddressedAsArrayLength() {
        assertEvaluate("array_length([2, 3, 4], 1)", 3);
    }

    @Test
    public void testEmptyArray() {
        assertEvaluateNull("array_upper(cast([] as array(integer)), 1)");
    }

    @Test
    public void testZeroArguments() {
        assertThatThrownBy(() -> assertEvaluateNull("array_upper()"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: array_upper(). " +
                                    "Possible candidates: array_upper(array(E), integer):integer");
    }

    @Test
    public void testOneArgument() {
        assertThatThrownBy(() -> assertEvaluateNull("array_upper([1])"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: array_upper(_array(1)), " +
                                    "no overload found for matching argument types: (integer_array).");
    }

    @Test
    public void testThreeArguments() {
        assertThatThrownBy(() -> assertEvaluateNull("array_upper([1], 2, [3])"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: array_upper(_array(1), 2, _array(3)), " +
                "no overload found for matching argument types: (integer_array, integer, integer_array).");
    }

    @Test
    public void testSecondArgumentNotANumber() {
        assertThatThrownBy(() -> assertEvaluateNull("array_upper([1], [2])"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: array_upper(_array(1), _array(2)), " +
                "no overload found for matching argument types: (integer_array, integer_array).");
    }

    @Test
    public void testFirstArgumentIsNull() {
        assertThatThrownBy(() -> assertEvaluateNull("array_upper(null, 1)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The inner type of the array argument `array_upper` function cannot be undefined");
    }
}
