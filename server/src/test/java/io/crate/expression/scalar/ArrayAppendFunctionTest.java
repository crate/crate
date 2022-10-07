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


import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class ArrayAppendFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeWithValueSymbols() throws Exception {
        assertNormalize("array_append([10, 20], 30)", isLiteral(Arrays.asList(10, 20, 30)));
    }

    @Test
    public void testNormalizeWithRefs() throws Exception {
        assertNormalize("array_append(long_array, 30)", isFunction(ArrayAppendFunction.NAME));
    }

    @Test
    public void testNullObjectArgument() throws Exception {
        assertNormalize("array_append([1, 2, 3], null)", isLiteral(Arrays.asList(1, 2, 3, null)));
    }

    @Test
    public void testNullArrayArgument() throws Exception {
        assertNormalize("array_append(null, 1)", isLiteral(List.of(1)));
    }

    @Test
    public void testNullArrayInnerTypeArgument() throws Exception {
        assertNormalize("array_append([null], 1)", isLiteral(Arrays.asList(null, 1)));
    }

    @Test
    public void testOneNullArrayInnerTypeArgumentNullObject() throws Exception {
        assertNormalize("array_append([null, 1], null)", isLiteral(Arrays.asList(null, 1, null)));
    }

    @Test
    public void testEmptyArrayArgument() throws Exception {
        assertNormalize("array_append([], 1)", isLiteral(List.of(1)));
    }

    @Test
    public void testDifferentConvertableInnerTypesLong() throws Exception {
        assertNormalize("array_append([1::integer], 2::long)", isLiteral(Arrays.asList(1L, 2L)));
    }

    @Test
    public void testDifferentConvertableInnerTypesDouble() throws Exception {
        assertNormalize("array_append([1::integer], 2::double)", isLiteral(Arrays.asList(1.0, 2.0)));
    }

    @Test
    public void testEmptyArrayNullObject() throws Exception {
        List<Object> expectedValue = new java.util.ArrayList<>();
        expectedValue.add(null);
        assertNormalize("array_append([], null)", isLiteral(expectedValue));
    }

    @Test
    public void testNullArrayNullObject() throws Exception {
        List<Object> expectedValue = new java.util.ArrayList<>();
        expectedValue.add(null);
        assertNormalize("array_append(null, null)", isLiteral(expectedValue));
    }

    @Test
    public void testNullArrayInnerTypeNullObject() throws Exception {
        assertNormalize("array_append([null], null)", isLiteral(Arrays.asList(null, null)));
    }
}
