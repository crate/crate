/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.aggregation.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.testing.DataTypeTesting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class TopKAggregationTest extends AggregationTestCase {

    @Test
    public void test_top_k_longs() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.LONG);
    }

    @Test
    public void test_top_k_doubles() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.DOUBLE);
    }

    @Test
    public void test_top_k_floats() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.FLOAT);
    }

    @Test
    public void test_top_k_strings() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.STRING);
    }

    @Test
    public void test_top_k_integer() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.INTEGER);
    }

    @Test
    public void test_top_k_shorts() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.SHORT);
    }

    @Test
    public void test_top_k_byte() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.BYTE);
    }

    @Test
    public void test_top_k_timestamps() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.TIMESTAMPZ);
        execute_top_k_without_and_with_doc_values(DataTypes.TIMESTAMP);
    }

    @Test
    public void test_top_k_ip() throws Exception {
        execute_top_k_without_and_with_doc_values(DataTypes.IP);
    }

    @Test
    public void test_top_k_invalid_limit_parameters() throws Exception {
        Object[][] dataWithInvalidLimit = {
            new Object[]{1L, -1},
        };

        assertThatThrownBy(() -> executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            dataWithInvalidLimit,
            true,
            List.of()
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Limit parameter for topk must be between 0 and 10_000. Got: -1");
    }

    @Test
    public void test_top_k_boolean() throws Exception {
        var data = new Object[][]{
            new Boolean[]{true},
            new Boolean[]{true},
            new Boolean[]{true},
            new Boolean[]{false},
            new Boolean[]{false},
        };

        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.BOOLEAN),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", true, "frequency", 3L),
                    Map.of("item", false, "frequency", 2L)
                )
            );
    }

    @Test
    public void test_top_k_boolean_with_limit() throws Exception {
        int limit = 1;

        var data = new Object[][]{
            new Object[]{true, limit},
            new Object[]{true, limit},
            new Object[]{true, limit},
            new Object[]{false, limit},
            new Object[]{false, limit},
        };

        var resultWithLimit = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.BOOLEAN, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of(Literal.of(1))
        );

        assertThat(resultWithLimit)
            .isEqualTo(
                List.of(
                    Map.of("item", true, "frequency", 3L)
                )
            );
    }

    private void execute_top_k_without_and_with_doc_values(DataType<?> dataType) throws Exception {
        var generator = DataTypeTesting.getDataGenerator(dataType);
        var first = generator.get();
        var second = generator.get();
        while (second.equals(first)) {
            second = generator.get();
        }
        var third = generator.get();
        while (third.equals(second) || (third.equals(first))) {
            third = generator.get();
        }

        Object[][] data = {
            new Object[]{first},
            new Object[]{second},
            new Object[]{second},
            new Object[]{third},
            new Object[]{third},
            new Object[]{third},
        };

        assertHasDocValueAggregator(TopKAggregation.NAME, List.of(dataType));

        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(dataType),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", third, "frequency", 3L),
                    Map.of("item", second, "frequency", 2L),
                    Map.of("item", first, "frequency", 1L)
                )
            );
        int limit = 2;

        Object[][] dataWithLimit = {
            new Object[]{first, limit},
            new Object[]{second, limit},
            new Object[]{second, limit},
            new Object[]{third, limit},
            new Object[]{third, limit},
            new Object[]{third, limit},
        };

        result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(dataType, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            dataWithLimit,
            true,
            List.of(Literal.of(limit))
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", third, "frequency", 3L),
                    Map.of("item", second, "frequency", 2L)
                )
            );
    }

}
