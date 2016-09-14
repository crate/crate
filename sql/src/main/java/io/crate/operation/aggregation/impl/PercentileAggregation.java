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

import com.google.common.base.Preconditions;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import io.crate.Streamer;
import io.crate.analyze.symbol.Function;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.CircuitBreakingException;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.*;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class PercentileAggregation extends AggregationFunction<PercentileAggregation.TDigestState, Object> {

    public static final String NAME = "percentile";
    private static final int COMPRESSION = 100;
    private final FunctionInfo info;

    static {
        DataTypes.register(TDigestStateType.ID, TDigestStateType.INSTANCE);
    }

    public static void register(AggregationImplModule mod) {
        mod.register(NAME, new PercentileAggregationFunctionResolver());
    }

    private static class PercentileAggregationFunctionResolver implements DynamicFunctionResolver {
        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() < 2 || dataTypes.size() > 2) {
                throw new IllegalArgumentException("percentile function requires at least 2 arguments");
            } else if (dataTypes.size() == 2 && dataTypes.get(1).equals(new ArrayType(DataTypes.DOUBLE))) {
                return new PercentileAggregation(new FunctionInfo(new FunctionIdent(NAME, dataTypes),
                        new ArrayType(DataTypes.DOUBLE), FunctionInfo.Type.AGGREGATE));
            } else {
                Preconditions.checkArgument(!dataTypes.get(1).equals(new ArrayType(DataTypes.UNDEFINED)),
                        "fraction should be in range of [0,1]");

                Preconditions.checkArgument(dataTypes.get(1).isConvertableTo(DataTypes.DOUBLE),
                        String.format("fraction cannot be converted to the needed type (%s)",
                        DataTypes.DOUBLE));

                return new PercentileAggregation(new FunctionInfo(new FunctionIdent(NAME, dataTypes),
                        DataTypes.DOUBLE, FunctionInfo.Type.AGGREGATE));
            }
        }
    }

    public PercentileAggregation(FunctionInfo info) {
        this.info = info;
    }

    public static class TDigestStateType extends DataType<TDigestState> implements Streamer<TDigestState>, DataTypeFactory {

        private static final int ID = 5120;
        public static final TDigestStateType INSTANCE = new TDigestStateType();

        private TDigestStateType() {}

        @Override
        public DataType<?> create() {
            return INSTANCE;
        }

        @Override
        public TDigestState readValueFrom(StreamInput in) throws IOException {
            return TDigestState.read(in);
        }

        @Override
        public void writeValueTo(StreamOutput out, Object v) throws IOException {
            TDigestState.write((TDigestState) v, out);
        }

        @Override
        public int id() {
            return ID;
        }

        @Override
        public String getName() {
            return "percentile_state";
        }

        @Override
        public Streamer<?> streamer() {
            return this;
        }

        @Override
        public TDigestState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (TDigestState) value;
        }

        @Override
        public int compareValueTo(TDigestState val1, TDigestState val2) {
            return 0;
        }
    }

    public static class TDigestState extends AVLTreeDigest {

        private final double compression;
        private final double[] fractions;

        public TDigestState(double compression, double[] fractions) {
            super(compression);
            this.compression = compression;
            this.fractions = fractions;
        }

        @Override
        public double compression() {
            return compression;
        }

        public double[] fractions() {
            return fractions;
        }

        public static void write(TDigestState state, StreamOutput out) throws IOException {
            out.writeDouble(state.compression);
            out.writeDoubleArray(state.fractions);
            out.writeVInt(state.centroidCount());
            for (Centroid centroid : state.centroids()) {
                out.writeDouble(centroid.mean());
                out.writeVLong(centroid.count());
            }
        }

        public static TDigestState read(StreamInput in) throws IOException {
            double compression = in.readDouble();
            double[] fractions = in.readDoubleArray();
            TDigestState state = new TDigestState(compression, fractions);
            int n = in.readVInt();
            for (int i = 0; i < n; i++) {
                state.add(in.readDouble(), in.readVInt());
            }
            return state;
        }

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
                fractions = ArrayType.toDoubleArray(values);
            } else {
                fractions = new double[1];
                fractions[0] = DataTypes.DOUBLE.value(argValue);
            }
            return new TDigestState(COMPRESSION, fractions);
        } else {
            return new TDigestState(COMPRESSION, null);
        }
    }

    @Override
    public TDigestState reduce(RamAccountingContext ramAccountingContext, TDigestState state1, TDigestState state2) {
        if (state1 == null) {
            if (state2 != null) {
                return state2;
            }
        }
        if (state2 != null) {
            state1.add(state2);
        }

        validateFraction(state1.fractions());
        return state1;
    }

    @Override
    public Object terminatePartial(RamAccountingContext ramAccountingContext, TDigestState state) {
        if (state.fractions() != null) {
            Double[] percentiles = new Double[state.fractions().length];
            if (state.fractions().length > 1) {
                for (int i = 0; i < state.fractions().length; i++) {
                    percentiles[i] = state.quantile(state.fractions()[i]);
                }
                return percentiles;
            } else {
                return state.quantile(state.fractions()[0]);
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
