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
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

public class SumAggregationTest extends AggregationTest {

    private Object executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("sum", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        DataType type = DataTypes.DOUBLE;
        assertEquals(type, getSum(type).info().returnType());

        type = DataTypes.FLOAT;
        assertEquals(type, getSum(type).info().returnType());

        type = DataTypes.LONG;
        assertEquals(type, getSum(type).info().returnType());
        assertEquals(type, getSum(DataTypes.INTEGER).info().returnType());
        assertEquals(type, getSum(DataTypes.SHORT).info().returnType());
        assertEquals(type, getSum(DataTypes.BYTE).info().returnType());
    }

    private FunctionImplementation getSum(DataType type) {
        return functions.get(null, "sum", ImmutableList.of(Literal.of(type, null)), SearchPath.pathWithPGCatalogAndDoc());
    }

    @Test
    public void testDouble() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}});

        assertEquals(1.0d, result);
    }

    @Test
    public void testFloat() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}});

        assertEquals(1.0f, result);
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}});

        assertEquals(10L, result);
    }

    @Test(expected = ArithmeticException.class)
    public void testLongOverflow() throws Exception {
        executeAggregation(DataTypes.LONG, new Object[][]{{Long.MAX_VALUE}, {1}});
    }

    @Test(expected = ArithmeticException.class)
    public void testLongUnderflow() throws Exception {
        executeAggregation(DataTypes.LONG, new Object[][]{{Long.MIN_VALUE}, {-1}});
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}});

        assertEquals(10L, result);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}});

        assertEquals(10L, result);
    }

    @Test
    public void testByte() throws Exception {
        Object result = executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 7}, {(byte) 3}});

        assertEquals(10L, result);
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        executeAggregation(DataTypes.GEO_POINT, new Object[][]{});
    }
}
