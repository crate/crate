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

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

class PercentileAggregation extends AggregationFunction<TDigestState, Object> {

    public static final String NAME = "percentile";

    static {
        DataTypes.register(TDigestStateType.ID, in -> TDigestStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            builder.add(
                    Signature.builder(NAME, FunctionType.AGGREGATE)
                            .argumentTypes(supportedType.getTypeSignature(),
                                    DataTypes.DOUBLE.getTypeSignature())
                            .returnType(DataTypes.DOUBLE.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                    PercentileAggregation::new
            );
            builder.add(
                    Signature.builder(NAME, FunctionType.AGGREGATE)
                            .argumentTypes(supportedType.getTypeSignature(),
                                    DataTypes.DOUBLE_ARRAY.getTypeSignature())
                            .returnType(DataTypes.DOUBLE_ARRAY.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                    PercentileAggregation::new
            );
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    private PercentileAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Nullable
    @Override
    public TDigestState newState(RamAccounting ramAccounting,
                                 Version indexVersionCreated,
                                 Version minNodeInCluster,
                                 MemoryManager memoryManager) {
        ramAccounting.addBytes(TDigestState.SHALLOW_SIZE);
        return TDigestState.createEmptyState();
    }

    @Override
    public TDigestState iterate(RamAccounting ramAccounting,
                                MemoryManager memoryManager,
                                TDigestState state,
                                Input<?>... args) throws CircuitBreakingException {
        if (state.isEmpty()) {
            Object fractionValue = args[1].value();
            initState(state, fractionValue, ramAccounting);
        }
        Double value = DataTypes.DOUBLE.sanitizeValue(args[0].value());
        if (value != null) {
            int sizeBefore = state.byteSize();
            state.add(value);
            int sizeDelta = state.byteSize() - sizeBefore;
            if (sizeDelta > 0) {
                ramAccounting.addBytes(sizeDelta);
            }
        }
        return state;
    }

    private void initState(TDigestState state, Object argValue, RamAccounting ramAccounting) {
        if (argValue != null) {
            if (argValue instanceof List<?> values) {
                if (values.isEmpty() || values.contains(null)) {
                    throw new IllegalArgumentException("no fraction value specified");
                }
                ramAccounting.addBytes(values.size() * Double.BYTES);
                state.fractions(toDoubleArray(values));
            } else {
                ramAccounting.addBytes(Double.BYTES);
                state.fractions(new double[]{DataTypes.DOUBLE.sanitizeValue(argValue)});
            }
        }
    }

    private static double[] toDoubleArray(List<?> values) {
        double[] result = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = DataTypes.DOUBLE.sanitizeValue(values.get(i));
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
        if (boundSignature.returnType() instanceof ArrayType) {
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
