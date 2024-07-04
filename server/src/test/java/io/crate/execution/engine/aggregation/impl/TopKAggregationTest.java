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

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataTypes;

public class TopKAggregationTest extends AggregationTestCase {


    @Test
    public void test_top_k_longs() throws Exception {
        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            new Object[][]{
                new Long[]{1L},
                new Long[]{2L},
                new Long[]{2L},
                new Long[]{3L},
                new Long[]{3L},
                new Long[]{3L},
            },
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
    public void test_top_k_longs_with_limit() throws Exception {
        int limit = 2;
        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG, DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            new Object[][]{
                new Object[]{1L, limit},
                new Object[]{2L, limit},
                new Object[]{2L, limit},
                new Object[]{3L, limit},
                new Object[]{3L, limit},
                new Object[]{3L, limit},
            },
            true,
            List.of()
        );

        assertThat(result)
            .isEqualTo(
                List.of(
                    Map.of("item", 3L, "frequency", 3L),
                    Map.of("item", 2L, "frequency", 2L)
                )
            );
    }

    @Test
    public void test_top_K_strings() throws Exception {
        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.STRING),
            DataTypes.UNTYPED_OBJECT,
            new Object[][]{
                new String[]{"1"},
                new String[]{"2"},
                new String[]{"2"},
                new String[]{"3"},
                new String[]{"3"},
                new String[]{"3"}
            },
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

    @Test
    public void test_top_K_boolean() throws Exception {
        var result = executeAggregation(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.BOOLEAN),
            DataTypes.UNTYPED_OBJECT,
            new Object[][]{
                new Boolean[]{true},
                new Boolean[]{true},
                new Boolean[]{true},
                new Boolean[]{false},
                new Boolean[]{false},

            },
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
}
