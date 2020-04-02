/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ComparisonChain;
import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.apache.commons.math3.util.FastMath;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class GeometricMeanAggregation extends AggregationFunction<GeometricMeanAggregation.GeometricMeanState, Double> {

    public static final String NAME = "geometric_mean";

    static {
        DataTypes.register(GeometricMeanStateType.ID, in -> GeometricMeanStateType.INSTANCE);
    }

    private static final List<DataType> SUPPORTED_TYPES = Lists2.concat(
        DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMPZ);

    public static void register(AggregationImplModule mod) {
        for (var supportedType : SUPPORTED_TYPES) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()),
                args -> new GeometricMeanAggregation(
                    new FunctionInfo(
                        new FunctionIdent(NAME, args),
                        DataTypes.DOUBLE,
                        FunctionInfo.Type.AGGREGATE))
            );
        }
    }

    public static class GeometricMeanState implements Comparable<GeometricMeanState>, Writeable {
        /**
         * Number of values that have been added
         */
        private long n;

        /**
         * The currently running value
         */
        private double value;

        public GeometricMeanState() {
            value = 0d;
            n = 0;
        }

        public GeometricMeanState(StreamInput in) throws IOException {
            n = in.readVLong();
            value = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(n);
            out.writeDouble(value);
        }

        private void addValue(double val) {
            this.value += FastMath.log(val);
            n++;
        }

        private void subValue(double val) {
            this.value -= FastMath.log(val);
            n--;
        }

        private Double value() {
            if (n > 0) {
                return FastMath.exp(value / n);
            } else {
                return null;
            }
        }

        private void merge(GeometricMeanState other) {
            this.value += other.value;
            this.n += other.n;
        }

        @Override
        public int compareTo(GeometricMeanState o) {
            return ComparisonChain.start()
                .compare(value, o.value)
                .compare(n, o.n)
                .result();
        }
    }

    public static class GeometricMeanStateType extends DataType<GeometricMeanState>
        implements Streamer<GeometricMeanState>, FixedWidthType {

        public static final GeometricMeanStateType INSTANCE = new GeometricMeanStateType();
        public static final int ID = 4096;

        @Override
        public int id() {
            return ID;
        }

        @Override
        public Precedence precedence() {
            return Precedence.UNDEFINED;
        }

        @Override
        public String getName() {
            return "geometric_mean_state";
        }

        @Override
        public Streamer<GeometricMeanState> streamer() {
            return this;
        }

        @Override
        public GeometricMeanState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (GeometricMeanState) value;
        }

        @Override
        public int compare(GeometricMeanState val1, GeometricMeanState val2) {
            return val1.compareTo(val2);
        }

        @Override
        public int fixedSize() {
            return 40;
        }

        @Override
        public GeometricMeanState readValueFrom(StreamInput in) throws IOException {
            return new GeometricMeanState(in);
        }

        @Override
        public void writeValueTo(StreamOutput out, GeometricMeanState v) throws IOException {
            v.writeTo(out);
        }
    }

    private final FunctionInfo info;

    public GeometricMeanAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Nullable
    @Override
    public GeometricMeanState newState(RamAccounting ramAccounting,
                                       Version indexVersionCreated,
                                       Version minNodeInCluster,
                                       MemoryManager memoryManager) {
        ramAccounting.addBytes(GeometricMeanStateType.INSTANCE.fixedSize());
        return new GeometricMeanState();
    }

    @Override
    public GeometricMeanState iterate(RamAccounting ramAccounting,
                                      MemoryManager memoryManager,
                                      GeometricMeanState state,
                                      Input... args) throws CircuitBreakingException {
        if (state != null) {
            Number value = (Number) args[0].value();
            if (value != null) {
                state.addValue(value.doubleValue());
            }
        }
        return state;
    }

    @Override
    public GeometricMeanState reduce(RamAccounting ramAccounting, GeometricMeanState state1, GeometricMeanState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.merge(state2);
        return state1;
    }

    @Override
    public Double terminatePartial(RamAccounting ramAccounting, GeometricMeanState state) {
        return state.value();
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public GeometricMeanState removeFromAggregatedState(RamAccounting ramAccounting,
                                                        GeometricMeanState previousAggState,
                                                        Input[] stateToRemove) {
        if (previousAggState != null) {
            Number value = (Number) stateToRemove[0].value();
            if (value != null) {
                previousAggState.subValue(value.doubleValue());
            }
        }
        return previousAggState;
    }

    @Override
    public DataType<?> partialType() {
        return GeometricMeanStateType.INSTANCE;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
