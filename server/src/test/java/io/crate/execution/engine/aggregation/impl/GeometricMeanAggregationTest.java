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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class GeometricMeanAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
                Signature.builder(GeometricMeanAggregation.NAME, FunctionType.AGGREGATE)
                        .argumentTypes(argumentType.getTypeSignature())
                        .returnType(DataTypes.DOUBLE.getTypeSignature())
                        .features(Scalar.Feature.DETERMINISTIC)
                        .build(),
                data,
                List.of()
        );
    }

    @Test
    public void test_functions_return_type_is_always_double_for_any_argument_type() {
        for (DataType<?> type : Stream.concat(
            DataTypes.NUMERIC_PRIMITIVE_TYPES.stream(),
            Stream.of(DataTypes.TIMESTAMPZ)).toList()) {

            FunctionImplementation stddev = nodeCtx.functions().get(
                null,
                GeometricMeanAggregation.NAME,
                List.of(Literal.of(type, null)),
                SearchPath.pathWithPGCatalogAndDoc()
            );
            assertThat(stddev.boundSignature().returnType()).isEqualTo(DataTypes.DOUBLE);
        }
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_supported_numeric_types() {
        for (var dataType : GeometricMeanAggregation.SUPPORTED_TYPES) {
            assertHasDocValueAggregator(GeometricMeanAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void withNullArg() throws Exception {
        assertThat(executeAggregation(DataTypes.DOUBLE, new Object[][]{{null}, {null}}))
            .isNull();
    }

    @Test
    public void testDouble() throws Exception {
        assertThat(executeAggregation(DataTypes.DOUBLE, new Object[][]{{1.0d}, {1000.0d}, {1.0d}, {null}}))
            .isEqualTo(9.999999999999998d);
    }

    @Test
    public void testFloat() throws Exception {
        assertThat(executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.7f}}))
            .isEqualTo(0.5277632097890468d);
    }

    @Test
    public void testInteger() throws Exception {
        assertThat(executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}}))
            .isEqualTo(4.58257569495584d);
    }

    @Test
    public void testLong() throws Exception {
        assertThat(executeAggregation(DataTypes.LONG, new Object[][]{{1L}, {3L}, {2L}}))
            .isEqualTo(1.8171205928321397d);
    }

    @Test
    public void testShort() throws Exception {
        assertThat(executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 0}, {(short) 3}, {(short) 1000}}))
            .isEqualTo(0d);
    }

    @Test
    public void testByte() throws Exception {
        assertThat(executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 1}, {(byte) 1}}))
            .isEqualTo(1.0d);
    }

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> executeAggregation(DataTypes.BOOLEAN, new Object[][]{}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: geometric_mean(INPUT(0))," +
                                     " no overload found for matching argument types: (boolean).");
    }
}
