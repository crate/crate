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

package io.crate.execution.engine.aggregation.impl;

import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.exceptions.CircuitBreakingException;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

class PercentileAggregation extends AggregationFunction<TDigestState, Object> {

    private static final String NAME = "percentile";

    static {
        DataTypes.register(TDigestStateType.ID, in -> TDigestStateType.INSTANCE);
    }

    public static void register(AggregationImplModule mod) {
        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()),
                args -> new PercentileAggregation(
                    new FunctionInfo(
                        new FunctionIdent(NAME, args),
                        DataTypes.DOUBLE,
                        FunctionInfo.Type.AGGREGATE))
            );
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature()
                ),
                args -> new PercentileAggregation(
                    new FunctionInfo(
                        new FunctionIdent(NAME, args),
                        DataTypes.DOUBLE_ARRAY,
                        FunctionInfo.Type.AGGREGATE))
            );
        }
    }

    private final FunctionInfo info;

    PercentileAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public TDigestState newState(RamAccounting ramAccounting,
                                 Version indexVersionCreated,
                                 Version minNodeInCluster,
                                 MemoryManager memoryManager) {
        return TDigestState.createEmptyState();
    }

    @Override
    public TDigestState iterate(RamAccounting ramAccounting,
                                MemoryManager memoryManager,
                                TDigestState state,
                                Input... args) throws CircuitBreakingException {
        if (state.isEmpty()) {
            Object fractionValue = args[1].value();
            initState(state, fractionValue);
        }
        Double value = DataTypes.DOUBLE.value(args[0].value());
        if (value != null) {
            state.add(value);
        }
        return state;
    }

    private void initState(TDigestState state, Object argValue) {
        if (argValue != null) {
            if (argValue instanceof List) {
                List values = (List) argValue;
                if (values.isEmpty() || values.contains(null)) {
                    throw new IllegalArgumentException("no fraction value specified");
                }
                state.fractions(toDoubleArray(values));
            } else {
                state.fractions(new double[]{DataTypes.DOUBLE.value(argValue)});
            }
        }
    }

    private static double[] toDoubleArray(List values) {
        double[] result = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = DataTypes.DOUBLE.value(values.get(i));
        }
        return result;
    }

    @Override
    public TDigestState reduce(RamAccounting ramAccounting, TDigestState state1, TDigestState state2) {
        if (state1.isEmpty()) {
            return state2;
        }

        if (!state2.isEmpty()) {
            state1.add(state2);
        }
        return state1;
    }

    @Override
    @Nullable
    public Object terminatePartial(RamAccounting ramAccounting, TDigestState state) {
        if (state.isEmpty()) {
            return null;
        }
        List<Double> percentiles = new ArrayList<>(state.fractions().length);
        if (info.returnType() instanceof ArrayType) {
            for (int i = 0; i < state.fractions().length; i++) {
                double percentile = state.quantile(state.fractions()[i]);
                if (Double.isNaN(percentile)) {
                    percentiles.add(null);
                } else {
                    percentiles.add(percentile);
                }
            }
            return percentiles;
        } else {
            double percentile = state.quantile(state.fractions()[0]);
            if (Double.isNaN(percentile)) {
                return null;
            } else {
                return percentile;
            }
        }
    }

    @Override
    public DataType<?> partialType() {
        return TDigestStateType.INSTANCE;
    }
}
