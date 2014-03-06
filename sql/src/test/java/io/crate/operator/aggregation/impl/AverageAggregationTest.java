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
import io.crate.DataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AverageAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("avg", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionIdent fi = new FunctionIdent("avg", ImmutableList.of(DataType.INTEGER));
        // Return type is fixed to Double
        assertEquals(DataType.DOUBLE, functions.get(fi).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataType.DOUBLE, new Object[][]{{0.7d}, {0.3d}});

        assertEquals(0.5d, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataType.FLOAT, new Object[][]{{0.7f}, {0.3f}});

        assertEquals(0.5d, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataType.INTEGER, new Object[][]{{7}, {3}});

        assertEquals(5d, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataType.LONG, new Object[][]{{7L}, {3L}});

        assertEquals(5d, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataType.SHORT, new Object[][]{{(short) 7}, {(short) 3}});

        assertEquals(5d, result[0][0]);
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        Object[][] result = executeAggregation(DataType.STRING, new Object[][]{{"Youri"}, {"Ruben"}});
    }
}
