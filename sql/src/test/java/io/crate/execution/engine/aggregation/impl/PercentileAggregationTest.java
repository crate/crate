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
import io.crate.analyze.symbol.Literal;
import io.crate.breaker.RamAccountingContext;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PercentileAggregationTest extends AggregationTest {

    private static final String NAME = "percentile";

    private Object[][] execSingleFractionPercentile(DataType valueType, Object[][] rows) throws Exception {
        String name = "percentile";
        return executeAggregation(name, valueType, rows, ImmutableList.of(valueType, DataTypes.DOUBLE));
    }

    private Object[][] execArrayFractionPercentile(DataType valueType, Object[][] rows) throws Exception {
        String name = "percentile";
        return executeAggregation(name, valueType, rows, ImmutableList.of(valueType, new ArrayType(DataTypes.DOUBLE)));
    }

    @Test
    public void testReturnTypes() throws Exception {
        assertEquals(DataTypes.DOUBLE,
            functions.getBuiltin(NAME, ImmutableList.of(DataTypes.DOUBLE, DataTypes.DOUBLE)).info().returnType());
        assertEquals(new ArrayType(DataTypes.DOUBLE),
            functions.getBuiltin(NAME, ImmutableList.of(DataTypes.DOUBLE, new ArrayType(DataTypes.DOUBLE))).info().returnType());
    }

    @Test
    public void testAllTypesReturnSameResult() throws Exception {
        for (DataType valueType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            Double[] fractions = {0.5, 0.8};
            Object[][] rowsWithSingleFraction = new Object[10][];
            Object[][] rowsWithFractionsArray = new Object[10][];
            for (int i = 0; i < rowsWithSingleFraction.length; i++) {
                rowsWithSingleFraction[i] = new Object[]{
                    valueType.value(i), fractions[0]
                };
                rowsWithFractionsArray[i] = new Object[]{
                    valueType.value(i), fractions
                };
            }
            Object[][] result = execSingleFractionPercentile(valueType, rowsWithSingleFraction);
            assertEquals(4.5, result[0][0]);
            result = execArrayFractionPercentile(valueType, rowsWithFractionsArray);
            assertTrue(result[0][0].getClass().isArray());
            assertEquals(2, ((Object[]) result[0][0]).length);
            assertEquals(4.5, ((Object[]) result[0][0])[0]);
            assertEquals(7.2, ((Object[]) result[0][0])[1]);
        }
    }

    @Test
    public void testNullPercentile() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, null},
            {10, null}
        });

        assertTrue(result[0][0] == null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPercentile() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, new Object[]{}},
            {10, new Object[]{}}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullMultiplePercentiles() throws Exception {
        Double[] fractions = new Double[]{0.25, null};
        Object[][] result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, fractions},
            {10, fractions}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePercentile() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, -1.2},
            {10, -1.2}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLargePercentile() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, 1.5},
            {10, 1.5}
        });
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.STRING, new Object[][]{
            {"Akira", 0.5},
            {"Tetsuo", 0.5}
        });
    }

    @Test
    public void testNullInputValuesReturnNull() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.LONG, new Object[][]{
            {null, 0.5},
            {null, 0.5}
        });
        assertEquals(result[0][0], null);
    }

    @Test
    public void testEmptyPercentileFuncWithEmptyRows() throws Exception {
        Object[][] result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{});
        assertThat(result[0][0], is(nullValue()));
    }

    public void testIterate() throws Exception {
        PercentileAggregation pa = new PercentileAggregation(mock(FunctionInfo.class));

        TDigestState state = pa.iterate(null, TDigestState.createEmptyState(), Literal.of(1), Literal.of(0.5));
        assertThat(state, is(notNullValue()));
        assertThat(state.fractions()[0], is(0.5));
    }

    @Test
    public void testReduceStage() throws Exception {
        PercentileAggregation pa = new PercentileAggregation(mock(FunctionInfo.class));

        // state 1 -> state 2
        TDigestState state1 = TDigestState.createEmptyState();
        TDigestState state2 = new TDigestState(100, new double[]{0.5});
        state2.add(20.0);
        TDigestState reducedState = pa.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0], is(0.5));
        assertThat(reducedState.centroidCount(), is(1));

        // state 2 -> state 1
        state1 = new TDigestState(100, new double[]{0.5});
        state1.add(22.0);
        state1.add(20.0);
        state2 = new TDigestState(100, new double[]{0.5});
        state2.add(21.0);
        reducedState = pa.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0], is(0.5));
        assertThat(reducedState.centroidCount(), is(3));
    }

    @Test
    public void testSingleItemFractionsArgumentResultsInArrayResult() {
        ArrayType doubleArray = new ArrayType(DataTypes.DOUBLE);
        AggregationFunction impl = (AggregationFunction<?, ?>) functions.getBuiltin(
            NAME, Arrays.asList(DataTypes.LONG, doubleArray));

        RamAccountingContext memoryCtx = new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));
        Object state = impl.newState(memoryCtx, Version.CURRENT, BigArrays.NON_RECYCLING_INSTANCE);
        Literal<Object[]> fractions = Literal.of(new Object[]{0.95}, doubleArray);
        impl.iterate(memoryCtx, state, Literal.of(10L), fractions);
        impl.iterate(memoryCtx, state, Literal.of(20L), fractions);
        Object result = impl.terminatePartial(memoryCtx, state);

        assertThat("result must be an array", result.getClass().isArray(), is(true));
    }
}
