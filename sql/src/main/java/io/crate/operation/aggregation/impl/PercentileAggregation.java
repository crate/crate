/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.CircuitBreakingException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Arrays;

public class PercentileAggregation extends AggregationFunction<TDigestState, Object> {

    public static final String NAME = "percentile";
    private static final int COMPRESSION = 100;
    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType<?> t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            mod.register(new PercentileAggregation(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(t, DataTypes.DOUBLE)), DataTypes.DOUBLE,
                FunctionInfo.Type.AGGREGATE)));
            mod.register(new PercentileAggregation(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.of(t, DataTypes.DOUBLE_ARRAY)), DataTypes.DOUBLE_ARRAY,
                FunctionInfo.Type.AGGREGATE)));
        }
    }

    public PercentileAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public TDigestState newState(RamAccountingContext ramAccountingContext) {
        return null;
    }

    @Override
    public TDigestState iterate(RamAccountingContext ramAccountingContext, TDigestState state, Input... args) throws CircuitBreakingException {
        if (state == null) {
            Object argValue = args[1].value();
            state = initState(argValue);
        }
        Double value = DataTypes.DOUBLE.value(args[0].value());
        if (value != null) {
            state.add(value);
        }
        return state;
    }

    private TDigestState initState(Object argValue) {
        if (argValue != null) {
            double[] fractions;
            if (argValue.getClass().isArray()) {
                Object[] values = (Object[]) argValue;
                if (values.length == 0 || Arrays.asList(values).contains(null)) {
                    throw new IllegalArgumentException("no fraction value specified");
                }
                fractions = toDoubleArray(values);
            } else {
                fractions = new double[1];
                fractions[0] = DataTypes.DOUBLE.value(argValue);
            }
            return new TDigestState(COMPRESSION, fractions);
        } else {
            return new TDigestState(COMPRESSION, null);
        }
    }

    private static double[] toDoubleArray(Object[] array) {
        Object value;
        double[] values = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            value = array[i];
            values[i] = DataTypes.DOUBLE.value(value);
        }
        return values;
    }

    @Override
    public TDigestState reduce(RamAccountingContext ramAccountingContext, TDigestState state1, TDigestState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 != null) {
            state1.add(state2);
            validateFraction(state1.fractions());
        }

        return state1;
    }

    @Override
    public Object terminatePartial(RamAccountingContext ramAccountingContext, TDigestState state) {
        if (state.fractions() != null) {
            Double[] percentiles = new Double[state.fractions().length];
            if (state.fractions().length > 1) {
                for (int i = 0; i < state.fractions().length; i++) {
                    Double percentile = state.quantile(state.fractions()[i]);
                    if (percentile.isNaN()) {
                        percentiles[i] = null;
                    } else {
                        percentiles[i] = percentile;
                    }
                }
                return percentiles;
            } else {
                Double percentile = state.quantile(state.fractions()[0]);
                if (percentile.isNaN()) {
                    return null;
                } else {
                    return percentile;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public DataType partialType() {
        return TDigestStateType.INSTANCE;
    }

    private static void validateFraction(double[] fractions) {
        if (fractions != null) {
            double fraction;
            for (int i = 0; i < fractions.length; i++) {
                fraction = fractions[i];
                if (fraction < 0 || fraction > 1) {
                    throw new IllegalArgumentException("fraction should be in range [0,1], got " + fraction);
                }
            }
        }
    }
}
