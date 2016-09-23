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
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class PercentileAggregationTest extends AggregationTest {

    private static final String NAME = "percentile";

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        String name = "percentile";
        return executeAggregation(name, dataType, data, ImmutableList.of(dataType, DataTypes.DOUBLE));
    }

    @Test
    public void testReturnTypes() throws Exception {
        FunctionIdent synopsis1 = new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.DOUBLE, DataTypes.DOUBLE));
        assertEquals(DataTypes.DOUBLE, functions.get(synopsis1).info().returnType());

        FunctionIdent synopsis2 = new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.DOUBLE, new ArrayType(DataTypes.DOUBLE)));
        assertEquals(new ArrayType(DataTypes.DOUBLE), functions.get(synopsis2).info().returnType());
    }

    @Test
    public void testAllTypesReturnSameResult() throws Exception {
        for (DataType dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            Double[] fractions = {0.5, 0.8};
            Object[][] rows = new Object[10][];
            Object[][] rowsArray = new Object[10][];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = new Object[]{
                    dataType.value(i), fractions[0]
                };
                rowsArray[i] = new Object[]{
                    dataType.value(i), fractions
                };
            }
            Object[][] result = executeAggregation(dataType, rows);
            assertEquals(4.5, result[0][0]);
            result = executeAggregation(dataType, rowsArray);
            assertTrue(result[0][0].getClass().isArray());
            assertEquals(2, ((Object[]) result[0][0]).length);
            assertEquals(4.5, ((Object[]) result[0][0])[0]);
            assertEquals(7.2, ((Object[]) result[0][0])[1]);
        }
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

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        Object[][] result = executeAggregation(DataTypes.STRING, new Object[][]{
            {"Akira", 0.5},
            {"Tetsuo", 0.5}
        });
    }

    @Test
    public void testNullInputValuesReturnNull() throws Exception {
        Object[][] result = executeAggregation(DataTypes.LONG, new Object[][]{
            {null, 0.5},
            {null, 0.5}
        });
        assertEquals(result[0][0], null);
    }

    @Test
    public void testNullStateResult() throws Exception {
        PercentileAggregation percAggr = new PercentileAggregation(new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.INTEGER, DataTypes.DOUBLE)), DataTypes.DOUBLE,
            FunctionInfo.Type.AGGREGATE));
        TDigestState state = percAggr.iterate(null, null, Literal.of(1), Literal.of(0.5));
        assertNotNull(state);
        assertThat(0.5, is(state.fractions()[0]));

        TDigestState reducedState = percAggr.reduce(null, null, state);
        assertEquals(state, reducedState);
    }
}


