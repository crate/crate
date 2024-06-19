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

package io.crate.execution.engine.aggregation.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.joda.time.Period;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class MaximumAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                "max",
                argumentType.getTypeSignature(),
                argumentType.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            data,
            List.of()
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(MaximumAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testDouble() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.8d}, {0.3d}});
        assertThat(result).isEqualTo(0.8d);
    }

    @Test
    public void testFloat() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.8f}, {0.3f}});
        assertThat(result).isEqualTo(0.8f);
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, new Object[][]{{8}, {3}});
        assertThat(result).isEqualTo(8);
    }

    @Test
    public void test_aggregate_double_zero() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.0}, {0.0}});
        assertThat(result).isEqualTo((0.0d));
    }

    @Test
    public void test_aggregate_float_zero() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.0f}, {0.0f}});
        assertThat(result).isEqualTo((0.0f));
    }

    @Test
    public void test_aggregate_min_float() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{- Float.MAX_VALUE}, {- Float.MAX_VALUE}});
        assertThat(result).isEqualTo((- Float.MAX_VALUE));
    }

    @Test
    public void test_aggregate_min_double() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{- Double.MAX_VALUE}, {- Double.MAX_VALUE}});
        assertThat(result).isEqualTo((- Double.MAX_VALUE));
    }

    @Test
    public void test_aggregate_min_long() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{Long.MIN_VALUE}, {Long.MIN_VALUE}});
        assertThat(result).isEqualTo((Long.MIN_VALUE));
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{8L}, {3L}});

        assertThat(result).isEqualTo(8L);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 8}, {(short) 3}});

        assertThat(result).isEqualTo((short) 8);
    }

    @Test
    public void test_max_with_byte_argument_type() throws Exception {
        assertThat(executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 1}, {(byte) 0}})).isEqualTo(((byte) 1));
    }

    @Test
    public void testString() throws Exception {
        Object result = executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}});

        assertThat(result).isEqualTo("Youri");
    }

    @Test
    public void test_max_on_interval() throws Exception {
        Object result = executeAggregation(DataTypes.INTERVAL, new Object[][] {
            { new Period(3, 2, 5, 7, 4, 30, 0, 0) },
            { new Period(3, 2, 5, 7, 5, 50, 0, 0) }
        });
        assertThat(result).isEqualTo(new Period(3, 2, 5, 7, 5, 50, 0, 0));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> executeAggregation(DataTypes.UNTYPED_OBJECT, new Object[][]{{new Object()}}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: max(INPUT(0)), no overload found for matching argument types: (object).");
    }
}
