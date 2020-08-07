/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ArbitraryAggregationTest extends AggregationTest {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                "arbitrary",
                argumentType.getTypeSignature(),
                argumentType.getTypeSignature()
            ),
            data
        );
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionImplementation arbitrary = functions.get(
            null, "arbitrary", ImmutableList.of(Literal.of(DataTypes.INTEGER, null)), SearchPath.pathWithPGCatalogAndDoc());
        assertEquals(DataTypes.INTEGER, arbitrary.info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] data = new Object[][]{{0.8d}, {0.3d}};
        Object result = executeAggregation(DataTypes.DOUBLE, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] data = new Object[][]{{0.8f}, {0.3f}};
        Object result = executeAggregation(DataTypes.FLOAT, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] data = new Object[][]{{8}, {3}};
        Object result = executeAggregation(DataTypes.INTEGER, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testLong() throws Exception {
        Object[][] data = new Object[][]{{8L}, {3L}};
        Object result = executeAggregation(DataTypes.LONG, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testShort() throws Exception {
        Object[][] data = new Object[][]{{(short) 8}, {(short) 3}};
        Object result = executeAggregation(DataTypes.SHORT, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testString() throws Exception {
        Object[][] data = new Object[][]{{"Youri"}, {"Ruben"}};
        Object result = executeAggregation(DataTypes.STRING, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testBoolean() throws Exception {
        Object[][] data = new Object[][]{{true}, {false}};
        Object result = executeAggregation(DataTypes.BOOLEAN, data);

        assertThat(result, is(oneOf(data[0][0], data[1][0])));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: arbitrary(INPUT(0))," +
                                        " no overload found for matching argument types: (object).");
        executeAggregation(DataTypes.UNTYPED_OBJECT, new Object[][]{{new Object()}});
    }
}
