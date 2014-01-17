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

package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.operator.aggregation.AggregationTest;
import org.cratedb.DataType;
import org.junit.Test;

import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AnyAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType) throws Exception {
        return executeAggregation("any", dataType);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionIdent fi = new FunctionIdent("any", ImmutableList.of(DataType.INTEGER));
        assertEquals(DataType.INTEGER ,functions.get(fi).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] data = new Object[][]{{0.8d}, {0.3d}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.DOUBLE);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] data = new Object[][]{{0.8f}, {0.3f}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.FLOAT);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] data = new Object[][]{{8}, {3}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.INTEGER);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testLong() throws Exception {
        Object[][] data = new Object[][]{{8L}, {3L}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.LONG);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testShort() throws Exception {
        Object[][] data = new Object[][]{{(short) 8}, {(short) 3}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.SHORT);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testString() throws Exception {
        Object[][] data = new Object[][]{{"Youri"}, {"Ruben"}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.STRING);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testBoolean() throws Exception {
        Object[][] data = new Object[][]{{true}, {false}};
        setUpTestData(data);
        Object[][] result = executeAggregation(DataType.BOOLEAN);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        setUpTestData(new Object[][]{{new Object()}});

        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("AggregationFunction implementation not found [FunctionIdent{name=any, argumentTypes=[object]}]");
        Object[][] result = executeAggregation(DataType.OBJECT);
    }
}
