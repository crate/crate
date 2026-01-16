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
import java.util.function.DoubleFunction;
import java.util.function.ToDoubleFunction;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionProvider.FunctionFactory;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntervalType;
import io.crate.types.TypeSignature;

class PercentileAggregation<T> extends AggregationFunction<TDigestState, Object> {

    public static final String NAME = "percentile";

    static {
        DataTypes.register(TDigestStateType.ID, _ -> TDigestStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        for (var supportedType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            builder.add(
                Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(
                        supportedType.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature()
                    )
                    .returnType(DataTypes.DOUBLE.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build(),
                PercentileAggregation::new
            );
            builder.add(
                Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(
                        supportedType.getTypeSignature(),
                        DataTypes.DOUBLE_ARRAY.getTypeSignature()
                    )
                    .returnType(DataTypes.DOUBLE_ARRAY.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build(),
                PercentileAggregation::new
            );

            // Optional 3rd `compression` setting argument
            builder.add(
                Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(
                        supportedType.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature()
                    )
                    .returnType(DataTypes.DOUBLE.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build(),
                PercentileAggregation::new
            );
            builder.add(
                Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(
                        supportedType.getTypeSignature(),
                        DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature()
                    )
                    .returnType(DataTypes.DOUBLE_ARRAY.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build(),
                PercentileAggregation::new
            );
        }

        // Interval type support
        FunctionFactory newIntervalPercentileAgg = (sig, boundSig) -> new PercentileAggregation<Period>(
            sig,
            boundSig,
            IntervalType.INSTANCE,
            value -> IntervalType.toStandardDuration(value).doubleValue(),
            x -> new Period((long) x).normalizedStandard(PeriodType.dayTime())
        );

        TypeSignature intervalArraySignature = new ArrayType<>(DataTypes.INTERVAL).getTypeSignature();

        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                )
                .returnType(DataTypes.INTERVAL.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            newIntervalPercentileAgg
        );

        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature()
                )
                .returnType(intervalArraySignature)
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            newIntervalPercentileAgg
        );

        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                )
                .returnType(DataTypes.INTERVAL.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            newIntervalPercentileAgg
        );

        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.DOUBLE_ARRAY.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                )
                .returnType(intervalArraySignature)
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            newIntervalPercentileAgg
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<T> valueType;
    private final ToDoubleFunction<T> valueToDouble;
    private final DoubleFunction<T> doubleToValue;

    private PercentileAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.valueType = (DataType<T>) signature.getArgumentDataTypes().get(0);
        this.valueToDouble = x -> ((Number) x).doubleValue();
        this.doubleToValue = x -> (T) (Double) x;
    }

    private PercentileAggregation(Signature signature,
                                  BoundSignature boundSignature,
                                  DataType<T> valueType,
                                  ToDoubleFunction<T> valueToDouble,
                                  DoubleFunction<T> doubleToValue) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.valueType = valueType;
        this.valueToDouble = valueToDouble;
        this.doubleToValue = doubleToValue;
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
            if (args.length > 2) {
                Double compression = DataTypes.DOUBLE.sanitizeValue(args[2].value());
                state = new TDigestState(compression, new double[]{});
            }
            initState(state, fractionValue, ramAccounting);
        }
        T value = valueType.sanitizeValue(args[0].value());
        if (value != null) {
            int delta = state.addGetSizeDelta(valueToDouble.applyAsDouble(value));
            if (delta > 0) {
                ramAccounting.addBytes(delta);
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
                ramAccounting.addBytes((long) values.size() * Double.BYTES);
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
        if (boundSignature.returnType() instanceof ArrayType) {
            List<T> percentiles = new ArrayList<>(state.fractions().length);
            for (int i = 0; i < state.fractions().length; i++) {
                double percentile = state.quantile(state.fractions()[i]);
                if (Double.isNaN(percentile)) {
                    percentiles.add(null);
                } else {
                    percentiles.add(doubleToValue.apply(percentile));
                }
            }
            return percentiles;
        } else {
            double percentile = state.quantile(state.fractions()[0]);
            if (Double.isNaN(percentile)) {
                return null;
            } else {
                return doubleToValue.apply(percentile);
            }
        }
    }

    @Override
    public DataType<?> partialType() {
        return TDigestStateType.INSTANCE;
    }
}
