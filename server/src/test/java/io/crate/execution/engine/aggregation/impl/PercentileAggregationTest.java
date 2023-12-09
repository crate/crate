/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.Version;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class PercentileAggregationTest extends AggregationTestCase {

    private Object execSingleFractionPercentile(DataType<?> argumentType, Object[][] rows) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                PercentileAggregation.NAME,
                argumentType.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            rows,
            List.of()
        );
    }

    private Object execArrayFractionPercentile(DataType<?> argumentType, Object[][] rows) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                PercentileAggregation.NAME,
                argumentType.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature()
            ),
            rows,
            List.of()
        );
    }

    private PercentileAggregation singleArgPercentile;
    private PercentileAggregation arraysPercentile;

    @Before
    public void initFunctions() throws Exception {
        singleArgPercentile = (PercentileAggregation) nodeCtx.functions().getQualified(
            Signature.aggregate(
                PercentileAggregation.NAME,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            List.of(DataTypes.DOUBLE, DataTypes.DOUBLE),
            DataTypes.DOUBLE
        );
        arraysPercentile = (PercentileAggregation) nodeCtx.functions().getQualified(
            Signature.aggregate(
                PercentileAggregation.NAME,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature()
            ),
            List.of(DataTypes.DOUBLE, DataTypes.DOUBLE_ARRAY),
            DataTypes.DOUBLE_ARRAY
        );
    }

    @Test
    public void testReturnTypes() throws Exception {
        assertThat(singleArgPercentile.boundSignature().returnType()).isEqualTo(DataTypes.DOUBLE);
        assertThat(arraysPercentile.boundSignature().returnType()).isEqualTo(DataTypes.DOUBLE_ARRAY);
    }

    @Test
    public void testSignleFractionAllTypesReturnSameResult() throws Exception {
        for (DataType<?> valueType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            List<Double> fractions = Arrays.asList(0.5, 0.8);
            Object[][] rowsWithSingleFraction = new Object[10][];
            for (int i = 0; i < rowsWithSingleFraction.length; i++) {
                rowsWithSingleFraction[i] = new Object[]{ valueType.sanitizeValue(i), fractions.get(0) };
            }
            assertThat(execSingleFractionPercentile(valueType, rowsWithSingleFraction)).isEqualTo(4.5);
        }
    }

    @Test
    public void testWithFractionsAllTypesReturnSameResult() throws Exception {
        for (DataType<?> valueType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            List<Double> fractions = Arrays.asList(0.5, 0.8);
            Object[][] rowsWithFractionsArray = new Object[10][];
            for (int i = 0; i < rowsWithFractionsArray.length; i++) {
                rowsWithFractionsArray[i] = new Object[]{ valueType.sanitizeValue(i), fractions };
            }
            assertThat(execArrayFractionPercentile(valueType, rowsWithFractionsArray))
                .isEqualTo(List.of(4.5, 7.5));
        }
    }

    @Test
    public void testNullPercentile() throws Exception {
        Object result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{
            {1, null},
            {10, null}
        });

        assertThat(result).isNull();
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

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> execSingleFractionPercentile(DataTypes.GEO_POINT, new Object[][]{}))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: percentile(INPUT(0), INPUT(0)), " +
                                    "no overload found for matching argument types: (geo_point, double precision).");
    }

    @Test
    public void testNullInputValuesReturnNull() throws Exception {
        Object result = execSingleFractionPercentile(DataTypes.LONG, new Object[][]{
            {null, 0.5},
            {null, 0.5}
        });
        assertThat(result).isNull();
    }

    @Test
    public void testEmptyPercentileFuncWithEmptyRows() throws Exception {
        Object result = execSingleFractionPercentile(DataTypes.INTEGER, new Object[][]{});
        assertThat(result).isNull();
    }

    @Test
    public void testIterate() throws Exception {
        PercentileAggregation pa = singleArgPercentile;
        TDigestState state = pa.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, TDigestState.createEmptyState(), Literal.of(1), Literal.of(0.5));
        assertThat(state).isNotNull();
        assertThat(state.fractions()[0]).isEqualTo(0.5);
    }

    @Test
    public void testReduceStage() throws Exception {
        PercentileAggregation pa = singleArgPercentile;

        // state 1 -> state 2
        TDigestState state1 = TDigestState.createEmptyState();
        TDigestState state2 = new TDigestState(100, new double[]{0.5});
        state2.add(20.0);
        TDigestState reducedState = pa.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0]).isEqualTo(0.5);
        assertThat(reducedState.centroidCount()).isEqualTo(1);

        // state 2 -> state 1
        state1 = new TDigestState(100, new double[]{0.5});
        state1.add(22.0);
        state1.add(20.0);
        state2 = new TDigestState(100, new double[]{0.5});
        state2.add(21.0);
        reducedState = pa.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0]).isEqualTo(0.5);
        assertThat(reducedState.centroidCount()).isEqualTo(3);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSingleItemFractionsArgumentResultsInArrayResult() {
        var impl = (AggregationFunction<Object, ?>) nodeCtx.functions().getQualified(
            Signature.aggregate(
                PercentileAggregation.NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature()
            ),
            List.of(DataTypes.LONG, DataTypes.DOUBLE_ARRAY),
            DataTypes.DOUBLE_ARRAY
        );

        RamAccounting ramAccounting = RamAccounting.NO_ACCOUNTING;
        Object state = impl.newState(ramAccounting, Version.CURRENT, Version.CURRENT, memoryManager);
        Literal<List<Double>> fractions = Literal.of(Collections.singletonList(0.95D), DataTypes.DOUBLE_ARRAY);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of(10L), fractions);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of(20L), fractions);
        Object result = impl.terminatePartial(ramAccounting, state);

        assertThat(result).isInstanceOf(List.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_percentile_accounts_memory_for_tdigeststate() throws Exception {
        var impl = (AggregationFunction<Object, ?>) nodeCtx.functions().getQualified(
            Signature.aggregate(
                PercentileAggregation.NAME,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature()
            ),
            List.of(DataTypes.LONG, DataTypes.DOUBLE_ARRAY),
            DataTypes.DOUBLE_ARRAY
        );
        RamAccounting ramAccounting = new RamAccounting() {

            long total = 0;

            @Override
            public void addBytes(long bytes) {
                total += bytes;
            }

            @Override
            public long totalBytes() {
                return total;
            }

            @Override
            public void release() {
                total = 0;
            }

            @Override
            public void close() {
                release();
            }
        };
        Object state = impl.newState(ramAccounting, Version.CURRENT, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(72L);
        Literal<List<Double>> fractions = Literal.of(Collections.singletonList(0.95D), DataTypes.DOUBLE_ARRAY);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of(10L), fractions);
        impl.iterate(ramAccounting, memoryManager, state, Literal.of(20L), fractions);
        assertThat(ramAccounting.totalBytes()).isEqualTo(104L);
    }
}
