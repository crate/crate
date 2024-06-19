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

public class ArrayToStringFunctionTest extends ScalarTestCase {

    @Test
    public void testZeroArguments() {
        assertThatThrownBy(() -> assertEvaluateNull("array_to_string()"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: array_to_string()");
    }

    @Test
    public void testOneArgument() {
        assertThatThrownBy(() -> assertEvaluateNull("array_to_string([1, 2])"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: array_to_string(_array(1, 2))," +
                " no overload found for matching argument types: (integer_array).");
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluateNull("array_to_string(null::text[], ',')");
    }

    @Test
    public void testEmptyArray() {
        assertEvaluate("array_to_string(cast([] as array(integer)), '')", "");
        assertEvaluate("array_to_string(cast([] as array(integer)), ',')", "");
        assertEvaluate("array_to_string(cast([] as array(integer)), '', '')", "");
    }

    @Test
    public void testNullArray() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("array_to_string(null, ',')");

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The inner type of the array argument `array_to_string` function cannot be undefined");
    }

    @Test
    public void testNullSeparator() {
        assertEvaluateNull("array_to_string([1, 2, 3], null)");
        assertEvaluateNull("array_to_string([1, null, 3], null, 2)");
    }

    @Test
    public void testEmptySeparator() {
        assertEvaluate("array_to_string([1, 2, 3], '')", "123");
        assertEvaluate("array_to_string([1, null, 3], '', '2')", "123");
    }

    @Test
    public void testSeparator() {
        assertEvaluate("array_to_string([1, 2, 3], ', ')", "1, 2, 3");
        assertEvaluate("array_to_string([1, null, 3], ', ')", "1, 3");
        assertEvaluate("array_to_string(['', '', ''], ', ')", ", , ");
        assertEvaluate("array_to_string(cast([null, null, null] as array(integer)), ', ')", "");
    }

    @Test
    public void testNullStringParameter() {
        assertEvaluate("array_to_string([1, 2, 3], ', ', 'xyz')", "1, 2, 3");
        assertEvaluate("array_to_string([1, null, 2], ', ', 'xyz')", "1, xyz, 2");
        assertEvaluate("array_to_string(cast([null, null, null] as array(integer)), ', ', 'xyz')", "xyz, xyz, xyz");
    }

    @Test
    public void testNullNullStringParameter() {
        assertEvaluate("array_to_string([1, 2, 3], ', ', null)", "1, 2, 3");
        assertEvaluate("array_to_string([1, null, 2], ', ', null)", "1, 2");
        assertEvaluate("array_to_string(cast([null, null, null] as array(integer)), ', ', null)", "");
    }

    @Test
    public void testPgArrayToStringWithFQNFunctionName() throws Exception {
        assertEvaluate("pg_catalog.array_to_string([1, 2, 3], ', ')", "1, 2, 3");
    }

}
