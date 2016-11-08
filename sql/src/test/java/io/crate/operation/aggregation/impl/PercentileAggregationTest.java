/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PercentileAggregationTest extends AggregationTest {

    private static final String NAME = "percentile";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation(NAME, dataType, data, ImmutableList.of(dataType, DataTypes.DOUBLE));
    }

    @Test
    public void testReturnTypes() throws Exception {
        FunctionIdent synopsis1 =
            new FunctionIdent(NAME, ImmutableList.of(DataTypes.DOUBLE, DataTypes.DOUBLE));
        assertEquals(DataTypes.DOUBLE, functions.get(synopsis1).info().returnType());

        FunctionIdent synopsis2 =
            new FunctionIdent(NAME, ImmutableList.of(DataTypes.DOUBLE, new ArrayType(DataTypes.DOUBLE)));
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
            assertThat(result[0][0], is(4.5));

            result = executeAggregation(dataType, rowsArray);
            assertThat(result[0][0].getClass().isArray(), is(true));
            assertThat(((Object[]) result[0][0])[0], is(4.5));
            assertThat(((Object[]) result[0][0])[1], is(7.2));
        }
    }

    @Test
    public void testNullPercentile() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{{1, null}});
        assertThat(result[0][0], is(nullValue()));
    }

    @Test
    public void testPercentileArrayWithNulls() throws Exception {
        Double[] fractions = new Double[]{0.25, null};
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{
            {1, fractions},
            {10, fractions}
        });
        assertThat(((Object[]) result[0][0])[0], is(3.25));
        assertThat(((Object[]) result[0][0])[1], is(nullValue()));
    }

    @Test
    public void testPercentileFunctionWithEmptyRows() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{});
        assertThat(((Object[]) result[0][0]).length, is(0));
    }

    @Test
    public void tesMultiplePercentiles() throws Exception {
        Double[] fractions = new Double[]{0.25, 0.5};
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{{1, fractions}, {10, fractions}});
        assertThat(((Object[]) result[0][0])[0], is(3.25));
        assertThat(((Object[]) result[0][0])[1], is(5.5));
    }

    @Test
    public void testNegativePercentile() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("should be in [0,1]"));
        executeAggregation(DataTypes.INTEGER, new Object[][]{{1, -0.2}});
    }

    @Test
    public void testNotValidPercentile() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("should be in [0,1]"));
        executeAggregation(DataTypes.INTEGER, new Object[][]{{1, 1.5}});
    }

    @Test
    public void testUnsupportedType() throws Exception {
        exception.expect(NullPointerException.class);
        executeAggregation(DataTypes.STRING, new Object[][]{{"Akira", 0.5}, {"Tetsuo", 0.5}});
    }

    @Test
    public void testNullValues() throws Exception {
        Object[][] result = executeAggregation(DataTypes.LONG, new Object[][]{{null, 0.5}, {null, 0.5}});
        assertThat(result[0][0], is(nullValue()));
    }

    @Test
    public void testIterate() throws Exception {
        PercentileAggregation pa = new PercentileAggregation(mock(FunctionInfo.class));
        TDigestState state = pa.iterate(null, new TDigestState(100, new Double[]{}), Literal.of(1), Literal.of(0.5));
        assertThat(state, is(notNullValue()));
        assertThat(state.fractions()[0], is(0.5));
    }

    @Test
    public void testReduceStage() throws Exception {
        PercentileAggregation pa = new PercentileAggregation(mock(FunctionInfo.class));

        // state 1 -> state 2
        TDigestState state1 = new TDigestState(100, new Double[]{});
        TDigestState state2 = new TDigestState(100, new Double[]{0.5});
        state2.add(20.0);
        TDigestState reducedState = pa.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0], is(0.5));
        assertThat(reducedState.centroidCount(), is(1));

        // state 2 -> state 1
        state1 = new TDigestState(100, new Double[]{0.5});
        state1.add(22.0);
        state1.add(20.0);
        state2 = new TDigestState(100, new Double[]{});
        reducedState = pa.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0], is(0.5));
        assertThat(reducedState.centroidCount(), is(2));
    }
}
