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

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFunctionException;

public class ArraySliceFunctionTest extends ScalarTestCase {

    @Test
    public void testFromAndTo() {
        assertEvaluate("[1, 2, 3, 4, 5][1:3]", List.of(1, 2, 3));
    }

    @Test
    public void testSliceAll() {
        assertEvaluate("[1, 2, 3, 4, 5][1:5]", List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void testSameFromAndTo() {
        assertEvaluate("[1, 2, 3, 4, 5][2: 2 ]", List.of(2));
    }

    @Test
    public void testFromOnly() {
        assertEvaluate("[1, 2, 3, 4, 5][2:]", List.of(2, 3, 4, 5));
    }

    @Test
    public void testFromIsMoreThanSizeOfArray() {
        assertEvaluate("[1, 2, 3, 4, 5][6:]", List.of());
    }

    @Test
    public void testFromIsEqualToSizeOfArray() {
        assertEvaluate("[1, 2, 3, 4, 5][5:]", List.of(5));
    }

    @Test
    public void testNoFromAndTo() {
        assertEvaluate("[1, 2, 3, 4, 5][:]", List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void testToIsMoreThanSizeOfArray() {
        assertEvaluate("[1, 2, 3, 4, 5][ 3 :100]", List.of(3, 4, 5));
    }

    @Test
    public void testFromAndToIsMoreThanSizeOfArray() {
        assertEvaluate("[1, 2, 3, 4, 5][ 20:100 ]", List.of());
    }

    @Test
    public void testFromIsBiggerThanTo() {
        assertEvaluate("[1, 2, 3, 4, 5][ 3 : 1 ]", List.of());
    }

    @Test
    public void testEmptyArray() {
        assertEvaluate("([]::array(integer))[1 :10]", List.of());
    }

    @Test
    public void testNullArray() {
        assertEvaluate("(null::array(integer))[1 :10]", List.of());
    }

    @Test
    public void testFromIsNull() {
        assertEvaluate("[1,2,3,4,5][null:3]", List.of(1, 2, 3));
    }

    @Test
    public void testToIsNull() {
        assertEvaluate("[1,2,3,4,5][3:null]", List.of(3, 4, 5));
    }

    @Test
    public void test_array_slice_qualified_name_usage() {
        assertEvaluate("array_slice([1, 2, 3, 4, 5], 3, 5)", List.of(3, 4, 5));
    }

    @Test
    public void testFromIsNotAnInteger() {
        assertThatThrownBy(() -> assertEvaluateNull("[1,2,3,4,5]['three':]"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'three'` of type `text` to type `integer`");
    }

    @Test
    public void testToIsNotAnInteger() {
        assertThatThrownBy(() -> assertEvaluateNull("[1,2,3,4,5][:'three']"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `'three'` of type `text` to type `integer`");
    }

    @Test
    public void testBaseIsNotAnArray() {
        assertThatThrownBy(() -> assertEvaluateNull("'not an array'[1:3]"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith(
                "Unknown function: array_slice('not an array', 1, 3), no overload found for matching argument types");
    }

    @Test
    public void testFromIsNegative() {
        assertThatThrownBy(() -> assertEvaluateNull("[1,2,3,4,5][-1:]"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Array index must be in range 1 to 2147483647");
    }

    @Test
    public void testToIsNegative() {
        assertThatThrownBy(() -> assertEvaluateNull("[1,2,3,4,5][:-1]"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Array index must be in range 1 to 2147483647");
    }

    @Test
    public void testFromIsBig() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][2147483648:]", List.of()))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `2147483648::bigint` of type `bigint` to type `integer`");
    }

    @Test
    public void testToIsBig() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][:2147483648]", List.of(1, 2, 3, 4, 5)))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast `2147483648::bigint` of type `bigint` to type `integer`");
    }

    @Test
    public void testFromIsBigExpression() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][2147483647+20:]", List.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void testToIsBigExpression() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][:2147483647+20]", List.of(1, 2, 3, 4, 5)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("integer overflow");
    }

    @Test
    public void testFromIsZero() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][0:]", List.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Array index must be in range 1 to 2147483647");
    }

    @Test
    public void testToIsZero() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][:0]", List.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Array index must be in range 1 to 2147483647");
    }

    @Test
    public void testFromIsZeroExpression() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][10-10:]", List.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Array index must be in range 1 to 2147483647");
    }

    @Test
    public void testToIsZeroExpression() {
        assertThatThrownBy(() -> assertEvaluate("[1,2,3,4,5][:10-10]", List.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Array index must be in range 1 to 2147483647");
    }
}
