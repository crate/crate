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

import io.crate.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PercentileAggregationTest extends AggregationTest {

    private static final String NAME = "percentile";

    private Object execSingleFractionPercentile(DataType<?> valueType, Object[][] rows) throws Exception {
        String name = "percentile";
        return executeAggregation(name, valueType, rows, List.of(valueType, DataTypes.DOUBLE));
    }

    private Object execArrayFractionPercentile(DataType<?> valueType, Object[][] rows) throws Exception {
        String name = "percentile";
        return executeAggregation(name, valueType, rows, List.of(valueType, DataTypes.DOUBLE_ARRAY));
    }

    private PercentileAggregation singleArgPercentile;
    private PercentileAggregation arraysPercentile;

    @Before
    public void initFunctions() throws Exception {
        singleArgPercentile = (PercentileAggregation) functions.getQualified(
            new FunctionIdent(NAME, Arrays.asList(DataTypes.DOUBLE, DataTypes.DOUBLE)));
        arraysPercentile = (PercentileAggregation) functions.getQualified(
            new FunctionIdent(NAME, Arrays.asList(DataTypes.DOUBLE, DataTypes.DOUBLE_ARRAY)));
    }

    @Test
    public void testReturnTypes() throws Exception {
        assertEquals(DataTypes.DOUBLE, singleArgPercentile.info().returnType());
        assertEquals(DataTypes.DOUBLE_ARRAY, arraysPercentile.info().returnType());
    }

    @Test
    public void testAllTypesReturnSameResult() throws Exception {
        for (DataType valueType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            List<Double> fractions = Arrays.asList(0.5, 0.8);
            Object[][] rowsWithSingleFraction = new Object[10][];
            Object[][] rowsWithFractionsArray = new Object[10][];
            for (int i = 0; i < rowsWithSingleFraction.length; i++) {
                rowsWithSingleFraction[i] = new Object[]{ valueType.value(i), fractions.get(0) };
                rowsWithFractionsArray[i] = new Object[]{ valueType.value(i), fractions };
            }
            Object result = execSingleFractionPercentile(valueType, rowsWithSingleFraction);
            assertEquals(4.5, result);
            result = execArrayFractionPercentile(valueType, rowsWithFractionsArray);
            assertThat(result, instanceOf(List.class));
            assertEquals(2, ((List) result).size());
            assertEquals(4.5, ((List) result).get(0));
            assertEquals(7.5, ((List) result).get(1));
        }
    }

    @Test
    public void testNullPercentile() throws Exception {
        Object result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, null},
            {10, null}
        });

        assertTrue(result == null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPercentile() throws Exception {
        execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, List.of()},
            {10, List.of()}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullMultiplePercentiles() throws Exception {
        List<Double> fractions = Arrays.asList(0.25, null);
        execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, fractions},
            {10, fractions}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePercentile() throws Exception {
        execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, -1.2},
            {10, -1.2}
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLargePercentile() throws Exception {
        execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, 1.5},
            {10, 1.5}
        });
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        execSingleFractionPercentile(DataTypes.GEO_POINT, new Object[][]{});
    }

    @Test
    public void testNullInputValuesReturnNull() throws Exception {
        Object result = execSingleFractionPercentile(DataTypes.LONG, new Object[][]{
            {null, 0.5},
            {null, 0.5}
        });
        assertEquals(result, null);
    }

    @Test
    public void testEmptyPercentileFuncWithEmptyRows() throws Exception {
        Object result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{});
        assertThat(result, is(nullValue()));
    }

    public void testIterate() throws Exception {
        PercentileAggregation pa = singleArgPercentile;
        TDigestState state = pa.iterate(null, memoryManager, TDigestState.createEmptyState(), Literal.of(1), Literal.of(0.5));
        assertThat(state, is(notNullValue()));
        assertThat(state.fractions()[0], is(0.5));
    }

    @Test
    public void testReduceStage() throws Exception {
        PercentileAggregation pa = singleArgPercentile;

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
        AggregationFunction impl = (AggregationFunction<?, ?>) functions.getQualified(
            new FunctionIdent(NAME, Arrays.asList(DataTypes.LONG, DataTypes.DOUBLE_ARRAY)));

        RamAccounting ramAccounting = RamAccounting.NO_ACCOUNTING;
        Object state = impl.newState(ramAccounting, Version.CURRENT, Version.CURRENT, memoryManager);
        Literal<List<Double>> fractions = Literal.of(Collections.singletonList(0.95D), DataTypes.DOUBLE_ARRAY);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of(10L), fractions);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of(20L), fractions);
        Object result = impl.terminatePartial(ramAccounting, state);

        assertThat("result must be an array", result, instanceOf(List.class));
    }
}
