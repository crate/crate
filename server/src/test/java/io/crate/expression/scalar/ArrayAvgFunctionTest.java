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
import java.util.List;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class ArrayAvgFunctionTest extends ScalarTestCase {

    @Test
    public void test_array_avg_on_long_array_returns_numeric() {
        assertEvaluate("array_avg(long_array)",
                       new BigDecimal(Long.MAX_VALUE),
                       Literal.of(List.of(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE),
                                  new ArrayType<>(DataTypes.LONG))
        );
    }

    @Test
    public void test_array_avg_on_short_array_returns_numeric() {
        assertEvaluate("array_avg(short_array)",
                       new BigDecimal(Short.MAX_VALUE),
                       Literal.of(List.of(Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE),
                                  new ArrayType<>(DataTypes.SHORT))
        );
    }

    @Test
    public void test_array_avg_on_byte_array_returns_numeric() {
        assertEvaluate("array_avg(?)",
                       new BigDecimal(Byte.MAX_VALUE),
                       Literal.of(List.of(Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE),
                                  new ArrayType<>(DataTypes.BYTE))
        );
    }

    @Test
    public void test_array_avg_on_int_array_returns_numeric() {
        assertEvaluate("array_avg(?)",
                       new BigDecimal(Integer.MAX_VALUE),
                       Literal.of(List.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                                  new ArrayType<>(DataTypes.INTEGER))
        );
    }

    @Test
    public void test_array_avg_on_float_array_returns_float() {
        assertEvaluate("array_avg([1.0, 2.0] :: real[])",
                       1.5f);
    }

    @Test
    public void test_array_avg_on_double_array_returns_double() {
        assertEvaluate("array_avg([1.0, 2.0] :: double precision[])",
                       1.5d);
    }

    @Test
    public void test_array_avg_on_numeric_array_returns_numeric() {
        assertEvaluate("array_avg(?)",
                       new BigDecimal("5.5"),
                       Literal.of(List.of(BigDecimal.ONE, BigDecimal.TEN), new ArrayType<>(DataTypes.NUMERIC))
        );
    }

    @Test
    public void test_array_avg_ignores_null_element_values() {
        assertEvaluate("array_avg([null, 1])", BigDecimal.ONE);
    }

    @Test
    public void test_all_elements_nulls_results_in_null() {
        assertEvaluateNull("array_avg([null, null]::integer[])");
    }

    @Test
    public void test_null_array_results_in_null() {
        assertEvaluateNull("array_avg(null::int[])");
    }

    @Test
    public void test_array_avg_returns_null_for_null_values() {
        assertEvaluateNull("array_avg(null)");
    }

    @Test
    public void test_empty_array_results_in_null() {
        assertEvaluateNull("array_avg(cast([] as array(integer)))");
    }

    @Test
    public void test_average_of_empty_array_is_null() {
        assertEvaluateNull("array_avg([])");
        assertEvaluateNull("array_avg([]::int[])");
    }
}
