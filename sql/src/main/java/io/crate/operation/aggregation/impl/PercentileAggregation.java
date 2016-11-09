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
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.CircuitBreakingException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

/**
 * The {@code PercentileAggregation} function computes a percentile over
 * numeric non-null values in a column.
 * <br/>
 *
 * The function expects a single fraction or an array of fractions and
 * a column name, e.g.
 * <pre>
 *      percentile(col, [.85, .9])
 * </pre>
 *
 * The function output for some corner cases are following:
 * <pre>
 *      percentile(col, [null, .85]) and
 *      percentile(col, null)
 *           returns a null result only for the null value fraction.
 * </pre>
 */
class PercentileAggregation extends AggregationFunction<TDigestState, Object> {

    private static final String NAME = "percentile";
    private static final int COMPRESSION = 100;
    private static final List<DataType> ALLOWED_FRACTION_DATA_TYPES = ImmutableList.of(
        DataTypes.DOUBLE, DataTypes.DOUBLE_ARRAY, DataTypes.UNDEFINED
    );

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType<?> fractionDT : ALLOWED_FRACTION_DATA_TYPES) {
            for (DataType<?> t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
                mod.register(new PercentileAggregation(new FunctionInfo(
                    new FunctionIdent(NAME, ImmutableList.of(t, fractionDT)), fractionDT, FunctionInfo.Type.AGGREGATE))
                );
            }
        }
    }

    PercentileAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public TDigestState newState(RamAccountingContext ramAccountingContext) {
        return new TDigestState(COMPRESSION, new Double[]{});
    }

    @Override
    public TDigestState iterate(RamAccountingContext ramAccountingContext, TDigestState state, Input... args)
        throws CircuitBreakingException {
        // The new states will be updated with fraction values on the iterate
        // method call. Despite that it is not required for further iterate
        // calls on the same state. This behaviour is caused by the current
        // design of the AggregationFunction.
        if (state.fractions().length == 0) {
            state.fractions(fractionsFromInputValue(args[1].value()));
        }

        Double value = DataTypes.DOUBLE.value(args[0].value());
        if (value != null) {
            state.add(value);
        }
        return state;
    }

    private Double[] fractionsFromInputValue(Object value) {
        if (value != null && value.getClass().isArray()) {
            Object[] values = (Object[]) value;
            return toDoubleFractionsArray(values);
        }
        return new Double[]{DataTypes.DOUBLE.value(value)};
    }

    private Double[] toDoubleFractionsArray(Object[] values) {
        Double[] fractions = new Double[values.length];
        for (int i = 0; i < values.length; i++) {
            Double fraction = DataTypes.DOUBLE.value(values[i]);
            fractions[i] = fraction;
        }
        return fractions;
    }

    @Override
    public TDigestState reduce(RamAccountingContext ramAccountingContext, TDigestState state1, TDigestState state2) {
        if (state1.fractions().length == 0) {
            state1.add(state2);
            return state1;
        }

        state2.add(state1);
        return state2;
    }

    @Override
    public Object terminatePartial(RamAccountingContext ramAccountingContext, TDigestState state) {
        if (state.fractions().length == 1) {
            return calcPercentile(state, 0);
        } else {
            return calcPercentiles(state);
        }
    }

    private Object calcPercentiles(TDigestState state) {
        Double[] percentiles = new Double[state.fractions().length];
        for (int i = 0; i < state.fractions().length; i++) {
            percentiles[i] = calcPercentile(state, i);
        }
        return percentiles;
    }

    private Double calcPercentile(TDigestState state, int idx) {
        Double fraction = state.fractions()[idx];
        if (fraction == null) {
            return null;
        }

        Double percentile = state.quantile(fraction);
        if (percentile.isNaN()) {
            return null;
        }
        return percentile;
    }

    @Override
    public DataType partialType() {
        return TDigestStateType.INSTANCE;
    }
}
