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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

public class PercentileAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        String name = "percentile";
        return executeAggregation(name, dataType, data, ImmutableList.of(dataType, DataTypes.DOUBLE));
    }

    @Test
    public void testReturnTypes() throws Exception {
        FunctionIdent synopsis1 = new FunctionIdent("percentile", ImmutableList.<DataType>of(DataTypes.LONG, DataTypes.DOUBLE));
        assertEquals(DataTypes.DOUBLE, functions.get(synopsis1).info().returnType());

        FunctionIdent synopsis2 = new FunctionIdent("percentile", ImmutableList.<DataType>of(DataTypes.LONG, new ArrayType(DataTypes.DOUBLE)));
        assertEquals(new ArrayType(DataTypes.DOUBLE), functions.get(synopsis2).info().returnType());
    }

    @Test
    public void testInteger() throws Exception {
        Double[] fractions = {0.5, 0.8};
        Object[][] result;

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, fractions[0]},
            {2, fractions[0]},
            {3, fractions[0]},
            {4, fractions[0]},
            {5, fractions[0]},
            {6, fractions[0]},
            {7, fractions[0]},
            {8, fractions[0]},
            {9, fractions[0]},
            {10, fractions[0]}
        });

        assertEquals(5.5, result[0][0]);

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, fractions},
            {2, fractions},
            {3, fractions},
            {4, fractions},
            {5, fractions},
            {6, fractions},
            {7, fractions},
            {8, fractions},
            {9, fractions},
            {10, fractions}
        });


        assertTrue(result[0][0].getClass().isArray());
        assertEquals(2, ((Object[])result[0][0]).length);
        assertEquals(5.5, ((Object[])result[0][0])[0]);
        assertEquals(8.2, ((Object[])result[0][0])[1]);
    }

    @Test
    public void testDouble() throws Exception {
        Double[] fractions = {0.5, 0.8};
        Object[][] result;

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1.0, fractions[0]},
            {2.0, fractions[0]},
            {3.0, fractions[0]},
            {4.0, fractions[0]},
            {5.0, fractions[0]},
            {6.0, fractions[0]},
            {7.0, fractions[0]},
            {8.0, fractions[0]},
            {9.0, fractions[0]},
            {10.0, fractions[0]}
        });

        assertEquals(5.5, result[0][0]);

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1.0, fractions},
            {2.0, fractions},
            {3.0, fractions},
            {4.0, fractions},
            {5.0, fractions},
            {6.0, fractions},
            {7.0, fractions},
            {8.0, fractions},
            {9.0, fractions},
            {10.0, fractions}
        });


        assertTrue(result[0][0].getClass().isArray());
        assertEquals(2, ((Object[])result[0][0]).length);
        assertEquals(5.5, ((Object[])result[0][0])[0]);
        assertEquals(8.2, ((Object[])result[0][0])[1]);
    }

    @Test
    public void testLong() throws Exception {
        Double[] fractions = {0.5, 0.8};
        Object[][] result;

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1L, fractions[0]},
            {2L, fractions[0]},
            {3L, fractions[0]},
            {4L, fractions[0]},
            {5L, fractions[0]},
            {6L, fractions[0]},
            {7L, fractions[0]},
            {8L, fractions[0]},
            {9L, fractions[0]},
            {10L, fractions[0]}
        });

        assertEquals(5.5, result[0][0]);

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1L, fractions},
            {2L, fractions},
            {3L, fractions},
            {4L, fractions},
            {5L, fractions},
            {6L, fractions},
            {7L, fractions},
            {8L, fractions},
            {9L, fractions},
            {10L, fractions}
        });


        assertTrue(result[0][0].getClass().isArray());
        assertEquals(2, ((Object[])result[0][0]).length);
        assertEquals(5.5, ((Object[])result[0][0])[0]);
        assertEquals(8.2, ((Object[])result[0][0])[1]);
    }

    @Test
    public void testShort() throws Exception {
        Double[] fractions = {0.5, 0.8};
        Object[][] result;

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {(short) 1, fractions[0]},
            {(short) 2, fractions[0]},
            {(short) 3, fractions[0]},
            {(short) 4, fractions[0]},
            {(short) 5, fractions[0]},
            {(short) 6, fractions[0]},
            {(short) 7, fractions[0]},
            {(short) 8, fractions[0]},
            {(short) 9, fractions[0]},
            {(short) 10, fractions[0]}
        });

        assertEquals(5.5, result[0][0]);

        result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {(short) 1, fractions},
            {(short) 2, fractions},
            {(short) 3, fractions},
            {(short) 4, fractions},
            {(short) 5, fractions},
            {(short) 6, fractions},
            {(short) 7, fractions},
            {(short) 8, fractions},
            {(short) 9, fractions},
            {(short) 10, fractions}
        });


        assertTrue(result[0][0].getClass().isArray());
        assertEquals(2, ((Object[])result[0][0]).length);
        assertEquals(5.5, ((Object[])result[0][0])[0]);
        assertEquals(8.2, ((Object[])result[0][0])[1]);
    }

    @Test
    public void testNullPercentile() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, null},
            {10, null}
        });

        assertTrue(result[0][0] == null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPercentile() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, new Object[]{}},
            {10, new Object[]{}}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullMultiplePercentiles() throws Exception {
        Double[] fractions = new Double[]{0.25, null};
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, fractions},
            {10, fractions}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePercentile() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, -1.2},
            {10, -1.2}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLargePercentile() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, 1.5},
            {10, 1.5}
        });
    }

    @Test(expected = NumberFormatException.class)
    public void testUnsupportedType() throws Exception {
        Object[][] result = executeAggregation(DataTypes.STRING, new Object[][]{
            {"Akira", 0.5},
            {"Tetsuo", 0.5}
        });
    }
}


