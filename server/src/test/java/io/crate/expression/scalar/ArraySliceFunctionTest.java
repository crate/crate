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
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'three'` of type `text` to type `integer`");
        assertEvaluateNull("[1,2,3,4,5]['three':]");
    }

    @Test
    public void testToIsNotAnInteger() {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'three'` of type `text` to type `integer`");
        assertEvaluateNull("[1,2,3,4,5][:'three']");
    }

    @Test
    public void testBaseIsNotAnArray() {
        expectedException.expect(UnsupportedFunctionException.class);
        expectedException.expectMessage("Unknown function: array_slice('not an array', 1, 3), no overload found for matching argument types");
        assertEvaluateNull("'not an array'[1:3]");
    }

    @Test
    public void testFromIsNegative() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        assertEvaluateNull("[1,2,3,4,5][-1:]");
    }

    @Test
    public void testToIsNegative() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        assertEvaluateNull("[1,2,3,4,5][:-1]");
    }

    @Test
    public void testFromIsBig() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `2147483648::bigint` of type `bigint` to type `integer`");
        assertEvaluate("[1,2,3,4,5][2147483648:]", List.of());
    }

    @Test
    public void testToIsBig() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `2147483648::bigint` of type `bigint` to type `integer`");
        assertEvaluate("[1,2,3,4,5][:2147483648]", List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void testFromIsBigExpression() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("[1,2,3,4,5][2147483647+20:]", List.of());
    }

    @Test
    public void testToIsBigExpression() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("integer overflow");
        assertEvaluate("[1,2,3,4,5][:2147483647+20]", List.of(1, 2, 3, 4, 5));
    }

    @Test
    public void testFromIsZero() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        assertEvaluate("[1,2,3,4,5][0:]", List.of());
    }

    @Test
    public void testToIsZero() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        assertEvaluate("[1,2,3,4,5][:0]", List.of());
    }

    @Test
    public void testFromIsZeroExpression() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        assertEvaluate("[1,2,3,4,5][10-10:]", List.of());
    }

    @Test
    public void testToIsZeroExpression() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        assertEvaluate("[1,2,3,4,5][:10-10]", List.of());
    }

}
