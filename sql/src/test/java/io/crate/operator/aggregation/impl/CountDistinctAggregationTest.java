/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import static org.junit.Assert.assertEquals;

public class CountDistinctAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("count", dataType, data, true);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionIdent fi = new FunctionIdent("count", ImmutableList.of(DataType.INTEGER), true);
        // Return type is fixed to Long
        assertEquals(DataType.LONG, functions.get(fi).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataType.DOUBLE, new Object[][]{{0.7d}, {0.3d}, {0.3d}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataType.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.3f}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataType.INTEGER, new Object[][]{{7}, {3}, {3}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataType.LONG, new Object[][]{{7L}, {3L}, {3L}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataType.SHORT, new Object[][]{{(short) 7}, {(short) 3}, {(short) 3}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testString() throws Exception {
        Object[][] result = executeAggregation(DataType.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {"Ruben"}});

        assertEquals(2L, result[0][0]);
    }

    @Test(expected = NullPointerException.class)
    public void testNoInput() throws Exception {
        // aka. COUNT(DISTINCT *)
        executeAggregation(null, new Object[][]{{}, {}});
    }
}
