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

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.List;

public class MaximumAggregationTest extends AggregationTest {

    private Object executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("max", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionImplementation max = functions.getQualified(
            Signature.aggregate("max", DataTypes.INTEGER.getTypeSignature(), DataTypes.INTEGER.getTypeSignature()),
            List.of(DataTypes.INTEGER)
        );
        assertEquals(DataTypes.INTEGER, max.info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.8d}, {0.3d}});

        assertEquals(0.8d, result);
    }

    @Test
    public void testFloat() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.8f}, {0.3f}});

        assertEquals(0.8f, result);
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, new Object[][]{{8}, {3}});

        assertEquals(8, result);
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{8L}, {3L}});

        assertEquals(8L, result);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 8}, {(short) 3}});

        assertEquals((short) 8, result);
    }

    @Test
    public void testString() throws Exception {
        Object result = executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}});

        assertEquals("Youri", result);
    }

    @Test
    public void testUnsupportedType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: max(object)");
        executeAggregation(DataTypes.UNTYPED_OBJECT, new Object[][]{{new Object()}});
    }
}
