/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class StdDevAggregationTest extends AggregationTest {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                "stddev",
                argumentType.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            data
        );
    }

    @Test
    public void test_functions_return_type_is_always_double_for_any_argument_type() {
        for (DataType<?> type : Stream.concat(
            DataTypes.NUMERIC_PRIMITIVE_TYPES.stream(),
            Stream.of(DataTypes.TIMESTAMPZ)).collect(Collectors.toList())) {

            FunctionImplementation stddev = nodeCtx.functions().get(
                null,
                StandardDeviationAggregation.NAME,
                List.of(Literal.of(type, null)),
                SearchPath.pathWithPGCatalogAndDoc()
            );
            assertThat(stddev.boundSignature().getReturnType().createType(), is(DataTypes.DOUBLE));
        }
    }

    @Test
    public void withNullArg() throws Exception {
        assertThat(executeAggregation(DataTypes.DOUBLE, new Object[][]{{null}, {null}}), is(nullValue()));
    }

    @Test
    public void withSomeNullArgs() throws Exception {
        assertThat(
            executeAggregation(DataTypes.DOUBLE, new Object[][]{{10.7d}, {42.9D}, {0.3d}, {null}}),
            is(18.13455878212156)
        );
    }

    @Test
    public void testDouble() throws Exception {
        assertThat(
            executeAggregation(DataTypes.DOUBLE, new Object[][]{{10.7d}, {42.9D}, {0.3d}}),
            is(18.13455878212156)
        );
    }

    @Test
    public void testFloat() throws Exception {
        assertThat(
            executeAggregation(DataTypes.FLOAT, new Object[][]{{1.5f}, {1.25f}, {1.75f}}),
            is(0.2041241452319315)
        );
    }

    @Test
    public void testInteger() throws Exception {
        assertThat(executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}}), is(2d));
    }

    @Test
    public void testLong() throws Exception {
        assertThat(executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}}), is(2d));
    }

    @Test
    public void testShort() throws Exception {
        assertThat(executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}}), is(2d));
    }

    @Test
    public void testByte() throws Exception {
        assertThat(executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 1}, {(short) 1}}), is(0d));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: stddev(INPUT(0))," +
                                        " no overload found for matching argument types: (geo_point).");
        executeAggregation(DataTypes.GEO_POINT, new Object[][]{});
    }
}
