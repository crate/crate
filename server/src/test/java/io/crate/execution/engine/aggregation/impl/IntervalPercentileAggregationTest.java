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
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class IntervalPercentileAggregationTest extends AggregationTestCase {

    private Object execSingleFractionPercentile(Object[][] rows) throws Exception {
        return executeAggregation(
            Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.INTERVAL.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            rows,
            List.of()
        );
    }

    private Object execArrayFractionPercentile(Object[][] rows) throws Exception {
        return executeAggregation(
            Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature())
                .returnType(new ArrayType<>(DataTypes.INTERVAL).getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            rows,
            List.of()
        );
    }

    private PercentileAggregation<Period> singleArgPercentile;
    private PercentileAggregation<Period> arraysPercentile;

    @Before
    @SuppressWarnings("unchecked")
    public void initFunctions() throws Exception {
        singleArgPercentile = (PercentileAggregation<Period>) nodeCtx.functions().getQualified(
            Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature())
                .returnType(DataTypes.INTERVAL.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            List.of(DataTypes.INTERVAL, DataTypes.DOUBLE),
            DataTypes.INTERVAL
        );
        arraysPercentile = (PercentileAggregation<Period>) nodeCtx.functions().getQualified(
            Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature())
                .returnType(new ArrayType<>(DataTypes.INTERVAL).getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            List.of(DataTypes.INTERVAL, DataTypes.DOUBLE_ARRAY),
            new ArrayType<>(DataTypes.INTERVAL)
        );
    }

    @Test
    public void testReturnTypes() throws Exception {
        assertThat(singleArgPercentile.boundSignature().returnType()).isEqualTo(DataTypes.INTERVAL);
        assertThat(arraysPercentile.boundSignature().returnType()).isEqualTo(new ArrayType<>(DataTypes.INTERVAL));
    }

    @Test
    public void testSingleFractionPercentileReturnsInterval() throws Exception {
        Object[][] rows = new Object[10][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{Period.hours(i + 1), 0.5};
        }
        Object result = execSingleFractionPercentile(rows);
        assertThat(result).isInstanceOf(Period.class);
        Period period = (Period) result;
        assertThat(period.getHours()).isBetween(5, 6);
    }

    @Test
    public void testArrayFractionPercentileReturnsIntervalArray() throws Exception {
        List<Double> fractions = Arrays.asList(0.5, 0.9);
        Object[][] rows = new Object[10][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{Period.hours(i + 1), fractions};
        }
        Object result = execArrayFractionPercentile(rows);
        assertThat(result).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Period> periods = (List<Period>) result;
        assertThat(periods).hasSize(2);
        assertThat(periods.get(0).getHours()).isBetween(5, 6);
        assertThat(periods.get(1).getHours()).isBetween(9, 10);
    }

    @Test
    public void testPercentileWithDaysAndHours() throws Exception {
        Object[][] rows = new Object[][]{
            {new Period().withDays(1).withHours(0), 0.5},
            {new Period().withDays(2).withHours(12), 0.5},
            {new Period().withDays(5).withHours(6), 0.5}
        };
        Object result = execSingleFractionPercentile(rows);
        assertThat(result).isInstanceOf(Period.class);
        Period period = (Period) result;
        assertThat(period.getDays()).isBetween(1, 5);
    }

    @Test
    public void testNullPercentile() throws Exception {
        Object result = execSingleFractionPercentile(new Object[][]{
            {Period.hours(1), null},
            {Period.hours(10), null}
        });
        assertThat(result).isNull();
    }

    @Test
    public void testEmptyFractionsThrows() throws Exception {
        assertThatThrownBy(() -> execSingleFractionPercentile(new Object[][]{
            {Period.hours(1), List.of()},
            {Period.hours(10), List.of()}
        }))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("no fraction value specified");
    }

    @Test
    public void testNullInputValuesReturnNull() throws Exception {
        Object result = execSingleFractionPercentile(new Object[][]{
            {null, 0.5},
            {null, 0.5}
        });
        assertThat(result).isNull();
    }

    @Test
    public void testEmptyRowsReturnNull() throws Exception {
        Object result = execSingleFractionPercentile(new Object[][]{});
        assertThat(result).isNull();
    }

    @Test
    public void testNegativePercentileThrows() throws Exception {
        assertThatThrownBy(() -> execSingleFractionPercentile(new Object[][]{
            {Period.hours(1), -1.2},
            {Period.hours(10), -1.2}
        }))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("q should be in [0,1], got -1.2");
    }

    @Test
    public void testTooLargePercentileThrows() throws Exception {
        assertThatThrownBy(() -> execSingleFractionPercentile(new Object[][]{
            {Period.hours(1), 1.5},
            {Period.hours(10), 1.5}
        }))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("q should be in [0,1], got 1.5");
    }

    @Test
    public void testIterate() throws Exception {
        TDigestState state = singleArgPercentile.iterate(
            RamAccounting.NO_ACCOUNTING,
            memoryManager,
            TDigestState.createEmptyState(),
            Literal.of(DataTypes.INTERVAL, Period.hours(1)),
            Literal.of(0.5)
        );
        assertThat(state).isNotNull();
        assertThat(state.fractions()[0]).isEqualTo(0.5);
    }

    @Test
    public void testReduceStage() throws Exception {
        TDigestState state1 = TDigestState.createEmptyState();
        TDigestState state2 = new TDigestState(100, new double[]{0.5});
        state2.add(3600000.0);
        TDigestState reducedState = singleArgPercentile.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0]).isEqualTo(0.5);
        assertThat(reducedState.centroidCount()).isEqualTo(1);

        state1 = new TDigestState(100, new double[]{0.5});
        state1.add(7200000.0);
        state1.add(3600000.0);
        state2 = new TDigestState(100, new double[]{0.5});
        state2.add(5400000.0);
        reducedState = singleArgPercentile.reduce(null, state1, state2);
        assertThat(reducedState.fractions()[0]).isEqualTo(0.5);
        assertThat(reducedState.centroidCount()).isEqualTo(3);
    }

    @Test
    public void testWithCompressionParameter() throws Exception {
        var signature = Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
            .argumentTypes(
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature())
            .returnType(DataTypes.INTERVAL.getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .build();

        double fraction = 0.5;
        double customCompression = 300.0;

        Object[][] rows = new Object[100][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{Period.hours(i + 1), fraction, customCompression};
        }

        Object result = executeAggregation(
            signature,
            signature.getArgumentDataTypes(),
            signature.getReturnType().createType(),
            rows,
            false,
            List.of()
        );

        assertThat(result).isInstanceOf(Period.class);
        Period period = (Period) result;
        int totalHours = period.getDays() * 24 + period.getHours();
        assertThat(totalHours).isBetween(50, 51);
    }

    @Test
    public void testPercentileWithMilliseconds() throws Exception {
        Object[][] rows = new Object[][]{
            {Period.millis(100), 0.5},
            {Period.millis(200), 0.5},
            {Period.millis(300), 0.5}
        };
        Object result = execSingleFractionPercentile(rows);
        assertThat(result).isInstanceOf(Period.class);
        Period period = (Period) result;
        assertThat(period.getMillis()).isBetween(180, 220);
    }

    @Test
    public void testPercentileOfTimestampDifference() throws Exception {
        Object[][] rows = new Object[10][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{Period.days(i + 1), 0.5};
        }
        Object result = execSingleFractionPercentile(rows);
        assertThat(result).isInstanceOf(Period.class);
        Period period = (Period) result;
        assertThat(period.getDays()).isBetween(5, 6);
        assertThat(period.getYears()).isEqualTo(0);
        assertThat(period.getMonths()).isEqualTo(0);
    }

    @Test
    public void testBoundaryPercentiles() throws Exception {
        Object[][] rows = new Object[][]{
            {Period.days(1), 0.0},
            {Period.days(5), 0.0},
            {Period.days(10), 0.0}
        };
        Object minResult = execSingleFractionPercentile(rows);
        assertThat(((Period) minResult).getDays()).isEqualTo(1);

        rows = new Object[][]{
            {Period.days(1), 1.0},
            {Period.days(5), 1.0},
            {Period.days(10), 1.0}
        };
        Object maxResult = execSingleFractionPercentile(rows);
        assertThat(((Period) maxResult).getDays()).isEqualTo(10);
    }

    @Test
    public void testSingleRowDataset() throws Exception {
        Object result = execSingleFractionPercentile(new Object[][]{
            {Period.days(5), 0.5}
        });
        assertThat(result).isInstanceOf(Period.class);
        assertThat(((Period) result).getDays()).isEqualTo(5);
    }

    @Test
    public void testMixedNullAndNonNullValues() throws Exception {
        Object result = execSingleFractionPercentile(new Object[][]{
            {null, 0.5},
            {Period.days(2), 0.5},
            {null, 0.5},
            {Period.days(4), 0.5},
            {Period.days(6), 0.5}
        });
        assertThat(result).isInstanceOf(Period.class);
        assertThat(((Period) result).getDays()).isEqualTo(4);
    }

    @Test
    public void testNegativeIntervals() throws Exception {
        Object result = execSingleFractionPercentile(new Object[][]{
            {Period.days(-5), 0.5},
            {Period.days(-3), 0.5},
            {Period.days(-1), 0.5}
        });
        assertThat(result).isInstanceOf(Period.class);
        assertThat(((Period) result).getDays()).isEqualTo(-3);
    }

    @Test
    public void testLargeIntervals() throws Exception {
        Object[][] rows = new Object[][]{
            {Period.days(365), 0.5},
            {Period.days(730), 0.5},
            {Period.days(1095), 0.5}
        };
        Object result = execSingleFractionPercentile(rows);
        assertThat(result).isInstanceOf(Period.class);
        Period period = (Period) result;
        assertThat(period.getDays()).isEqualTo(730);
        assertThat(period.getYears()).isEqualTo(0);
    }

    @Test
    public void testArrayFractionsWithCompression() throws Exception {
        var signature = Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
            .argumentTypes(
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature())
            .returnType(new ArrayType<>(DataTypes.INTERVAL).getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .build();

        List<Double> fractions = Arrays.asList(0.25, 0.5, 0.75);
        double compression = 200.0;

        Object[][] rows = new Object[100][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[]{Period.hours(i + 1), fractions, compression};
        }

        Object result = executeAggregation(
            signature,
            signature.getArgumentDataTypes(),
            signature.getReturnType().createType(),
            rows,
            false,
            List.of()
        );

        assertThat(result).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Period> periods = (List<Period>) result;
        assertThat(periods).hasSize(3);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_percentile_accounts_memory_for_tdigeststate() throws Exception {
        var impl = (PercentileAggregation<Period>) nodeCtx.functions().getQualified(
            Signature.builder(PercentileAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature())
                .returnType(new ArrayType<>(DataTypes.INTERVAL).getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            List.of(DataTypes.INTERVAL, DataTypes.DOUBLE_ARRAY),
            new ArrayType<>(DataTypes.INTERVAL)
        );
        io.crate.testing.PlainRamAccounting ramAccounting = new io.crate.testing.PlainRamAccounting();
        Object state = impl.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(120L);
        Literal<List<Double>> fractions = Literal.of(Collections.singletonList(0.95D), DataTypes.DOUBLE_ARRAY);
        impl.iterate(ramAccounting, memoryManager, (TDigestState) state,
            Literal.of(DataTypes.INTERVAL, Period.hours(10)), fractions);
        impl.iterate(ramAccounting, memoryManager, (TDigestState) state,
            Literal.of(DataTypes.INTERVAL, Period.hours(20)), fractions);
        assertThat(ramAccounting.totalBytes()).isEqualTo(192L);
    }
}
