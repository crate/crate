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

import org.junit.Test;

import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataTypes;

public class TopKTest extends AggregationTestCase {

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
        assertThat(result).isEqualTo(List.of(List.of(3L, 3L), List.of(2L, 2L), List.of(1L, 1L)));
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
        assertThat(result).isEqualTo(List.of(List.of("3", 3L), List.of("2", 2L), List.of("1", 1L)));
    }
}
