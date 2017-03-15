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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

public class StdDevAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("stddev", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        for (DataType<?> type : Iterables.concat(DataTypes.NUMERIC_PRIMITIVE_TYPES, Arrays.asList(DataTypes.TIMESTAMP))) {
            FunctionIdent fi = new FunctionIdent();
            // Return type is fixed to Double
            assertEquals(DataTypes.DOUBLE,
                getFunction("stddev", ImmutableList.of(type)).info().returnType());
        }
    }

    @Test
    public void withNullArg() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{null}, {null}});
        assertNull(result[0][0]);
    }

    @Test
    public void withSomeNullArgs() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{10.7d}, {42.9D}, {0.3d}, {null}});
        assertEquals(18.13455878212156, result[0][0]);
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{10.7d}, {42.9D}, {0.3d}});

        assertEquals(18.13455878212156, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataTypes.FLOAT, new Object[][]{{1.5f}, {1.25f}, {1.75f}});

        assertEquals(0.2041241452319315, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}});

        assertEquals(2d, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}});

        assertEquals(2d, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}});

        assertEquals(2d, result[0][0]);
    }

    @Test
    public void testByte() throws Exception {
        Object[][] result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 1}, {(short) 1}});

        assertEquals(0d, result[0][0]);
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}});
    }
}
