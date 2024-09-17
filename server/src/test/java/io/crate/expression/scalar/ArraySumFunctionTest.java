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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import io.crate.expression.symbol.Literal;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ArraySumFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_returns_sum_of_elements() {

        // This test picks up random numbers but controls that overflow will not happen (overflow case is checked in another test).

        List<DataType<?>> typesToTest = new ArrayList<>(DataTypes.NUMERIC_PRIMITIVE_TYPES);
        typesToTest.add(DataTypes.NUMERIC);

        for (DataType type : typesToTest) {
            var valuesToTest = TestingHelpers.getRandomsOfType(1, 10, type);

            DataType inputDependantOutputType = DataTypes.LONG;
            if (type == DataTypes.FLOAT || type == DataTypes.DOUBLE || type == DataTypes.NUMERIC) {
                inputDependantOutputType = type;
            } else {
                // check potential overflow and get rid of numbers causing overflow
                long sum = 0;
                for (int i = 0; i < valuesToTest.size(); i++) {
                    if (valuesToTest.get(i) != null) {
                        long nextNum = ((Number) valuesToTest.get(i)).longValue();
                        try {
                            sum = Math.addExact(sum, nextNum);
                        } catch (ArithmeticException e) {
                            valuesToTest = valuesToTest.subList(0, i); // excluding i
                            break;
                        }
                    }
                }
            }
            KahanSummationForDouble kahanSummationForDouble = new KahanSummationForDouble();
            var optional = valuesToTest.stream()
                .filter(Objects::nonNull)
                .reduce((o1, o2) -> {
                    if (o1 instanceof BigDecimal) {
                        return ((BigDecimal) o1).add((BigDecimal) o2);
                    } else if (o1 instanceof Double || o1 instanceof Float) {
                        return kahanSummationForDouble.sum(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
                    } else {
                        return DataTypes.LONG.implicitCast(o1) + DataTypes.LONG.implicitCast(o2);
                    }
                });
            var expected = inputDependantOutputType.implicitCast(optional.orElse(null));


            String expression = String.format(Locale.ENGLISH, "array_sum(?::%s[])", type.getName());
            assertEvaluate(expression, expected, Literal.of(valuesToTest, new ArrayType<>(type)));
        }
    }

    @Test
    public void test_array_big_numbers_no_casting_results_in_exception() {
        Assertions.assertThatThrownBy(() -> assertEvaluate(
            "array_sum(?)",
            null,
            Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE), new ArrayType<>(DataTypes.LONG))
        ))
            .isExactlyInstanceOf(ArithmeticException.class)
            .hasMessageContaining("long overflow");
    }

    @Test
    public void test_array_big_numbers_casting_to_numeric_returns_sum() {
        assertEvaluate("array_sum(cast(long_array as array(numeric)))",
                       new BigDecimal("18446744073709551614"),
                       Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE), new ArrayType<>(DataTypes.LONG))
        );
    }

    @Test
    public void test_array_first_element_null_returns_sum() {
        assertEvaluate("array_sum([null, 1])", 1L);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluateNull("array_sum(cast([null, null] as array(integer)))");
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluateNull("array_sum(null::int[])");
    }

    @Test
    public void test_null_array_given_directly_results_in_null() {
        assertEvaluateNull("array_sum(null)");
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluateNull("array_sum([])");
        assertEvaluateNull("array_sum(cast([] as array(integer)))");
    }
}
