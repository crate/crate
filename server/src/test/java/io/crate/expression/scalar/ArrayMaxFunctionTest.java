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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Literal;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ArrayMaxFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_returns_max_element() {
        List<DataType> typesToTest = new ArrayList(DataTypes.PRIMITIVE_TYPES);
        typesToTest.add(DataTypes.NUMERIC);

        for (DataType type : typesToTest) {
            var valuesToTest = TestingHelpers.getRandomsOfType(2, 10, type);

            var optional = valuesToTest.stream()
                .filter(o -> o != null)
                .max((o1, o2) -> type.compare(o1, o2));
            var expected = optional.orElse(null);

            String expression = String.format(Locale.ENGLISH, "array_max(?::%s[])", type.getName());
            assertEvaluate(expression, expected, Literal.of(valuesToTest, new ArrayType<>(type)));
        }
    }

    @Test
    public void test_array_first_element_null_returns_max() {
        assertEvaluate("array_max([null, 1])", 1);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluateNull("array_max(cast([null, null] as array(integer)))");
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluateNull("array_max(null::int[])");
    }

    @Test
    public void test_null_array_given_directly_results_in_null() {
        assertEvaluateNull("array_max(null)");
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluateNull("array_max(cast([] as array(integer)))");
    }

    @Test
    public void test_empty_array_given_directly_throws_exception() {
        Assertions.assertThatThrownBy(() -> assertEvaluate("array_max([])", null))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageContaining(
                "Invalid arguments in: array_max([]) with (undefined_array). Valid types: ");
    }

}
