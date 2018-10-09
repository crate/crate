/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar;

import org.junit.Test;

public class ArrayUpperFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testSecondArgumentIsNull() {
        assertEvaluate("array_upper([1, 2], null)", null);
    }

    @Test
    public void testSingleDimensionArrayValidDimension() {
        assertEvaluate("array_upper([4, 5], 1)", 2);
    }

    @Test
    public void testSingleDimensionArrayInvalidDimension() {
        assertEvaluate("array_upper([4, 5], 3)", null);
    }

    @Test
    public void testMultiDimensionArrayValidDimension() {
        assertEvaluate("array_upper([[1, 2, 3], [3, 4]], 2)", 2);
    }

    @Test
    public void testMultiDimensionArrayInvalidDimension() {
        assertEvaluate("array_upper([[1, 2, 3], [3, 4]], 3)", null);
    }

    @Test
    public void testCanBeAddressedAsArrayLength() {
        assertEvaluate("array_length([2, 3, 4], 1)", 3);
    }

    @Test
    public void testEmptyArray() {
        assertEvaluate("array_upper(cast([] as array(integer)), 1)", null);
    }
}
