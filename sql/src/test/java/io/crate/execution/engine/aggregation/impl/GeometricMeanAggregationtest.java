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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.List;

public class GeometricMeanAggregationtest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("geometric_mean", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        for (DataType<?> type : Iterables.concat(DataTypes.NUMERIC_PRIMITIVE_TYPES, List.of(DataTypes.TIMESTAMPZ))) {
            // Return type is fixed to Double
            assertEquals(DataTypes.DOUBLE,
                getGeometricMean(type).info().returnType());
        }
    }

    private FunctionImplementation getGeometricMean(DataType<?> type) {
        return functions.get(
            null, "geometric_mean", ImmutableList.of(Literal.of(type, null)), SearchPath.pathWithPGCatalogAndDoc());
    }

    @Test
    public void withNullArg() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{null}, {null}});
        assertNull(result[0][0]);
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{1.0d}, {1000.0d}, {1.0d}, {null}});

        assertEquals(9.999999999999998d, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.7f}});

        assertEquals(0.5277632097890468d, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}});

        assertEquals(4.58257569495584d, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataTypes.LONG, new Object[][]{{1L}, {3L}, {2L}});

        assertEquals(1.8171205928321397d, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 0}, {(short) 3}, {(short) 1000}});

        assertEquals(0d, result[0][0]);
    }

    @Test
    public void testByte() throws Exception {
        Object[][] result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 1}, {(short) 1}});

        assertEquals(1.0d, result[0][0]);
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        Object[][] result = executeAggregation(DataTypes.BOOLEAN, new Object[][]{{true}, {false}});
    }
}
