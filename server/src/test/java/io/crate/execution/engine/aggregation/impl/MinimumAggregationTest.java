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

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.metadata.FunctionType;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.testing.DataTypeTesting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class MinimumAggregationTest extends AggregationTestCase {

    private record NumericTestCase(String name, NumericType type, Object[][] data, BigDecimal expected) {}

    // Numeric values with the precision <= 18 are stored as long values
    // in the column store, so we're testing both cases.
    // See NumericStorage for more info.
    private static List<NumericTestCase> testNumericParameters() {
        return List.of(
            new NumericTestCase(
                "compact precision, simple case",
                new NumericType(6, 4),
                new Object[][]{{new BigDecimal("97.6543")}, {new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            ),
            new NumericTestCase(
                "large precision, simple case",
                new NumericType(22, 4),
                new Object[][]{{new BigDecimal("97.6543")}, {new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            ),
            // See: https://github.com/crate/crate/pull/17371
            new NumericTestCase(
                "compact precision, string sorting different",
                new NumericType(6, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("9.00")}},
                new BigDecimal("9.00")
            ),
            // See: https://github.com/crate/crate/pull/17371
            new NumericTestCase(
                "large precision, string sorting different",
                new NumericType(22, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("9.00")}},
                new BigDecimal("9.00")
            ),
            new NumericTestCase(
                "compact precision, single input",
                new NumericType(6, 4),
                new Object[][]{{new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            ),
            new NumericTestCase(
                "large precision, single input",
                new NumericType(22, 4),
                new Object[][]{{new BigDecimal("97.6542")}},
                new BigDecimal("97.6542")
            ),
            new NumericTestCase(
                "empty input",
                new NumericType(22, 2),
                new Object[][]{},
                null
            ),
            new NumericTestCase(
                "null input",
                new NumericType(22, 2),
                new Object[][]{{null}},
                null
            ),
            new NumericTestCase(
                "equal values",
                new NumericType(22, 2),
                new Object[][]{{new BigDecimal("10.00")}, {new BigDecimal("10.00")}},
                new BigDecimal("10.00")
            ),
            new NumericTestCase(
                "min value comes first",
                new NumericType(6, 4),
                new Object[][]{{new BigDecimal("97.6542")}, {new BigDecimal("97.6543")}},
                new BigDecimal("97.6542")
            )
        );
    }

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
                Signature.builder(MinimumAggregation.NAME, FunctionType.AGGREGATE)
                        .argumentTypes(argumentType.getTypeSignature())
                        .returnType(argumentType.getTypeSignature())
                        .features(Feature.DETERMINISTIC)
                        .build(),
                data,
                List.of()
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(MinimumAggregation.NAME, List.of(dataType));
        }
        assertHasDocValueAggregator(MinimumAggregation.NAME, List.of(new NumericType(6, 2)));
    }

    @Test
    public void test_numeric_aggregation_not_supported_without_scale_or_precision() {
        assertThatThrownBy(() -> getDocValueAggregator(MinimumAggregation.NAME, List.of(new NumericType(6, null))))
            .isExactlyInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> getDocValueAggregator(MinimumAggregation.NAME, List.of(new NumericType(null, null))))
            .isExactlyInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testNumeric() throws Exception {
        for (var tc : testNumericParameters()) {
            Object result = executeAggregation(tc.type(), tc.data());
            assertThat(result)
                .as(tc.name())
                .isEqualTo(tc.expected());
        }
    }

    private <T> void assert_min_on_primitive_type(DataType<T> type) throws Exception {
        Supplier<T> dataGenerator = DataTypeTesting.getDataGenerator(type);
        T val1 = dataGenerator.get();
        T val2;
        do {
            val2 = dataGenerator.get();
        } while (type.compare(val1, val2) == 0);

        Object[][] data = new Object[][] {
            { val1 },
            { val2 }
        };
        Object result = executeAggregation(type, data);
        if (type.compare(val1, val2) < 0) {
            assertThat(result).isEqualTo(val1);
        } else {
            assertThat(result).isEqualTo(val2);
        }
    }

    @Test
    public void test_min_on_primitive_types() throws Exception {
        for (var type : DataTypes.PRIMITIVE_TYPES) {
            assert_min_on_primitive_type(type);
        }
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
    public void test_min_with_byte_argument_type() throws Exception {
        assertThat(executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 1}, {(byte) 0}})).isEqualTo(((byte) 0));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> executeAggregation(DataTypes.UNTYPED_OBJECT, new Object[][]{{new Object()}}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Invalid arguments in: min(INPUT(0)) with (object). Valid types: ");
    }
}
