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
import io.crate.types.DataTypes;

public class TopKAggregationTest extends AggregationTestCase {

    @Test
    public void test_top_k_longs() throws Exception {
        Object[][] data = {
            new Long[]{1L},
            new Long[]{2L},
            new Long[]{2L},
            new Long[]{3L},
            new Long[]{3L},
            new Long[]{3L},
        };

        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", 3L, "frequency", 3L),
                    Map.of("item", 2L, "frequency", 2L),
                    Map.of("item", 1L, "frequency", 1L)
                )
            );
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

        Object[][] data = {
            new Object[]{1L},
        };
    }

    public void test_top_k_longs_with_limit() throws Exception {
        int limit = 2;

        Object[][] dataWithLimit = {
            new Object[]{1L, limit},
            new Object[]{2L, limit},
            new Object[]{2L, limit},
            new Object[]{3L, limit},
            new Object[]{3L, limit},
            new Object[]{3L, limit},
        };

        var resultWithLimit = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG, DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            dataWithLimit,
            true,
            List.of(Literal.of(limit))
        );

        assertThat(resultWithLimit)
            .isEqualTo(
                List.of(
                    Map.of("item", 3L, "frequency", 3L),
                    Map.of("item", 2L, "frequency", 2L)
                )
            );
    }

    @Test
    public void test_top_k_doubles() throws Exception {
        Object[][] data = {
            new Double[]{1.0D},
            new Double[]{2.0D},
            new Double[]{2.0D},
            new Double[]{3.0D},
            new Double[]{3.0D},
            new Double[]{3.0D},
        };

        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.DOUBLE),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", 3.0D, "frequency", 3L),
                    Map.of("item", 2.0D, "frequency", 2L),
                    Map.of("item", 1.0D, "frequency", 1L)
                )
            );
    }

    public void test_top_k_doubles_with_limit() throws Exception {
        int limit = 2;

        Object[][] dataWithLimit = {
            new Object[]{1.0D, limit},
            new Object[]{2.0D, limit},
            new Object[]{2.0D, limit},
            new Object[]{3.0D, limit},
            new Object[]{3.0D, limit},
            new Object[]{3.0D, limit},
        };

        var resultWithLimit = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.DOUBLE, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            dataWithLimit,
            true,
            List.of(Literal.of(limit))
        );

        assertThat(resultWithLimit)
            .isEqualTo(
                List.of(
                    Map.of("item", 3.0D, "frequency", 3L),
                    Map.of("item", 2.0D, "frequency", 2L)
                )
            );
    }

    @Test
    public void test_top_k_floats() throws Exception {
        Object[][] data = {
            new Float[]{1.0F},
            new Float[]{2.0F},
            new Float[]{2.0F},
            new Float[]{3.0F},
            new Float[]{3.0F},
            new Float[]{3.0F},
        };

        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.FLOAT),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", 3.0F, "frequency", 3L),
                    Map.of("item", 2.0F, "frequency", 2L),
                    Map.of("item", 1.0F, "frequency", 1L)
                )
            );
    }

    public void test_top_k_floats_with_limit() throws Exception {
        int limit = 2;

        Object[][] dataWithLimit = {
            new Object[]{1.0F, limit},
            new Object[]{2.0F, limit},
            new Object[]{2.0F, limit},
            new Object[]{3.0F, limit},
            new Object[]{3.0F, limit},
            new Object[]{3.0F, limit},
        };

        var resultWithLimit = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.FLOAT, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            dataWithLimit,
            true,
            List.of(Literal.of(limit))
        );

        assertThat(resultWithLimit)
            .isEqualTo(
                List.of(
                    Map.of("item", 3.0F, "frequency", 3L),
                    Map.of("item", 2.0F, "frequency", 2L)
                )
            );
    }


    @Test
    public void test_top_k_strings() throws Exception {
        Object[][] data = {
            new String[]{"1"},
            new String[]{"2"},
            new String[]{"2"},
            new String[]{"3"},
            new String[]{"3"},
            new String[]{"3"}
        };

        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.STRING),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", "3", "frequency", 3L),
                    Map.of("item", "2", "frequency", 2L),
                    Map.of("item", "1", "frequency", 1L)
                )
            );
    }

    public void test_top_k_strings_with_limit() throws Exception {
        int limit = 2;

        Object[][] data = {
            new Object[]{"1", limit},
            new Object[]{"2", limit},
            new Object[]{"2", limit},
            new Object[]{"3", limit},
            new Object[]{"3", limit},
            new Object[]{"3", limit},
        };

        var resultWithLimit = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.STRING, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of(Literal.of(limit))
        );

        assertThat(resultWithLimit)
            .isEqualTo(
                List.of(
                    Map.of("item", "3", "frequency", 3L),
                    Map.of("item", "2", "frequency", 2L)
                )
            );
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
}
