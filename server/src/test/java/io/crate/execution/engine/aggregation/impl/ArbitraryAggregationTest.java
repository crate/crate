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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ArbitraryAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                ArbitraryAggregation.NAME,
                argumentType.getTypeSignature(),
                argumentType.getTypeSignature()
            ),
            data,
            List.of()
        );
    }

    @Test
    public void test_return_type_must_be_equal_to_argument_type() {
        var arbitraryFunction = nodeCtx.functions().get(
            null,
            ArbitraryAggregation.NAME,
            List.of(Literal.of(DataTypes.INTEGER, null)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(
            arbitraryFunction.boundSignature().returnType(),
            is(DataTypes.INTEGER)
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(CountAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_string_based_types() {
        for (var dataType : List.of(DataTypes.STRING, DataTypes.IP)) {
            assertHasDocValueAggregator(CountAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] data = new Object[][]{{0.8d}, {0.3d}};
        assertThat(executeAggregation(DataTypes.DOUBLE, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] data = new Object[][]{{0.8f}, {0.3f}};
        assertThat(executeAggregation(DataTypes.FLOAT, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] data = new Object[][]{{8}, {3}};
        assertThat(executeAggregation(DataTypes.INTEGER, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testLong() throws Exception {
        Object[][] data = new Object[][]{{8L}, {3L}};
        assertThat(executeAggregation(DataTypes.LONG, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testShort() throws Exception {
        Object[][] data = new Object[][]{{(short) 8}, {(short) 3}};
        assertThat(executeAggregation(DataTypes.SHORT, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testString() throws Exception {
        Object[][] data = new Object[][]{{"Youri"}, {"Ruben"}};
        assertThat(executeAggregation(DataTypes.STRING, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testBoolean() throws Exception {
        Object[][] data = new Object[][]{{true}, {false}};
        assertThat(executeAggregation(DataTypes.BOOLEAN, data), is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        expectedException.expect(UnsupportedFunctionException.class);
        expectedException.expectMessage("Unknown function: arbitrary(INPUT(0))," +
                                        " no overload found for matching argument types: (object).");
        executeAggregation(DataTypes.UNTYPED_OBJECT, new Object[][]{{new Object()}});
    }

    @Test
    public void test_can_use_any_value_as_name() throws Exception {
        Signature aggregate = Signature.aggregate(
            "any_value",
            DataTypes.INTEGER.getTypeSignature(),
            DataTypes.INTEGER.getTypeSignature()
        );
        Object result = executeAggregation(aggregate, new Object[][] { new Object[] { 1 } }, List.of());
        assertThat(result).isEqualTo(1);
    }
}
