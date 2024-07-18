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

import io.crate.expression.symbol.Literal;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataTypes;

public class TopKAggregationTest extends AggregationTestCase {

    @Test
    public void test_top_k_longs_with_and_without_doc_values() throws Exception {

        Object[][] data = {
            new Long[]{1L},
            new Long[]{2L},
            new Long[]{2L},
            new Long[]{3L},
            new Long[]{3L},
            new Long[]{3L},
        };

        var result = executeAggregationWithoutDocValues(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
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

        var docValueResult = executeAggregationWithDocValues(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            List.of()
        );

        assertThat(result).isEqualTo(docValueResult);

        var resultWithLimit = executeAggregationWithoutDocValues(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG, DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            List.of(Literal.of(2))
        );

        assertThat(resultWithLimit)
            .isEqualTo(
                List.of(
                    Map.of("item", 3L, "frequency", 3L),
                    Map.of("item", 2L, "frequency", 2L)
                )
            );


        var docValueResultWithLimit = executeAggregationWithDocValues(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            List.of(Literal.of(2))
        );

        assertThat(resultWithLimit).isEqualTo(docValueResultWithLimit);
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

        var result = executeAggregationWithoutDocValues(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.STRING),
            DataTypes.UNTYPED_OBJECT,
            data,
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

        var docValueResult = executeAggregationWithDocValues(
            TopKAggregation.PARAMETER_SIGNATURE,
            List.of(DataTypes.STRING),
            DataTypes.UNTYPED_OBJECT,
            data,
            List.of()
        );

        assertThat(result).isEqualTo(docValueResult);

    }

    @Test
    public void test_top_k_boolean() throws Exception {
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
