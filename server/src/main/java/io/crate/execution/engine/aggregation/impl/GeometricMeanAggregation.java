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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.collect.ComparisonChain;

import org.apache.commons.math3.util.FastMath;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FixedWidthType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;

public class GeometricMeanAggregation extends AggregationFunction<GeometricMeanAggregation.GeometricMeanState, Double> {

    public static final String NAME = "geometric_mean";

    static {
        DataTypes.register(GeometricMeanStateType.ID, in -> GeometricMeanStateType.INSTANCE);
    }

    static final List<DataType<?>> SUPPORTED_TYPES = Lists2.concat(
        DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMPZ);

    public static void register(AggregationImplModule mod) {
        for (var supportedType : SUPPORTED_TYPES) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.DOUBLE.getTypeSignature()
                ),
                GeometricMeanAggregation::new
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GeometricMeanState that = (GeometricMeanState) o;
            return Objects.equals(value(), that.value());
        }

        @Override
        public int hashCode() {
            return Objects.hash(value());
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
        public GeometricMeanState sanitizeValue(Object value) {
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

    private final Signature signature;
    private final Signature boundSignature;

    public GeometricMeanAggregation(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
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
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        Reference reference = aggregationReferences.get(0);
        if (!reference.hasDocValues()) {
            return null;
        }
        switch (reference.valueType().id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(GeometricMeanStateType.INSTANCE.fixedSize());
                        return new GeometricMeanState();
                    },
                    (values, state) -> state.addValue(values.nextValue())
                );
            case FloatType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(GeometricMeanStateType.INSTANCE.fixedSize());
                        return new GeometricMeanState();
                    },
                    (values, state) -> {
                        var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                        state.addValue(value);
                    }
                );
            case DoubleType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(GeometricMeanStateType.INSTANCE.fixedSize());
                        return new GeometricMeanState();
                    },
                    (values, state) -> {
                        var value = NumericUtils.sortableLongToDouble((values.nextValue()));
                        state.addValue(value);
                    }
                );
            default:
                return null;
        }
    }
}
