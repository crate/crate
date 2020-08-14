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

package io.crate.execution.engine.aggregation.impl;

import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;


public class ArrayAggTest extends AggregationTest {

    @Test
    public void test_array_agg_adds_all_integer_values_to_array() throws Exception {
        var result = executeAggregation(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.INTEGER),
            DataTypes.INTEGER_ARRAY,
            new Object[][]{
                new Object[]{20},
                new Object[]{null},
                new Object[]{42},
                new Object[]{24}
            }
        );
        assertThat((List<?>) result, contains(20, null, 42, 24));
    }

    @Test
    public void test_array_agg_adds_all_float_values_to_array() throws Exception {
        var result = executeAggregation(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.FLOAT),
            DataTypes.FLOAT_ARRAY,
            new Object[][]{
                new Object[]{2.0f},
                new Object[]{null},
                new Object[]{2.4f}
            }
        );
        assertThat((List<?>) result, contains(2.0f, null, 2.4f));
    }

    @Test
    public void test_array_agg_adds_all_double_to_array() throws Exception {
        var result = executeAggregation(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.DOUBLE),
            DataTypes.DOUBLE_ARRAY,
            new Object[][]{
                new Object[]{2.0d},
                new Object[]{null},
                new Object[]{2.4d}
            }
        );
        assertThat((List<?>) result, contains(2.0d, null, 2.4d));
    }

    @Test
    public void test_array_agg_adds_all_text_values_to_array() throws Exception {
        var result = executeAggregation(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.STRING),
            DataTypes.STRING_ARRAY,
            new Object[][]{
                new Object[]{"a"},
                new Object[]{null},
                new Object[]{"b"}
            }
        );
        assertThat((List<?>) result, contains("a", null, "b"));
    }

    @Test
    public void test_array_agg_return_type_is_array_of_argument_type() {
        DataType<?> returnType = functions.getQualified(
            ArrayAgg.SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.BIGINT_ARRAY
        ).boundSignature().getReturnType().createType();
        assertThat(returnType, Matchers.is(DataTypes.BIGINT_ARRAY));
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(ArrayAgg.NAME, List.of(dataType));
        }
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_string_based_types() {
        for (var dataType : List.of(DataTypes.STRING, DataTypes.IP)) {
            assertHasDocValueAggregator(ArrayAgg.NAME, List.of(dataType));
        }
    }
}
