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

package io.crate.execution.engine.aggregation.impl.average;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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

public class AverageAggregation extends AggregationFunction<AverageAggregation.AverageState, Double> {

    public static final String[] NAMES = new String[]{"avg", "mean"};
    public static final String NAME = NAMES[0];

    static {
        DataTypes.register(AverageStateType.ID, in -> AverageStateType.INSTANCE);
    }

    static final List<DataType<?>> SUPPORTED_TYPES = Lists2.concat(
        DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMPZ);

    /**
     * register as "avg" and "mean"
     */
    public static void register(AggregationImplModule mod) {
        for (var functionName : NAMES) {
            for (var supportedType : SUPPORTED_TYPES) {
                mod.register(
                    Signature.aggregate(
                        functionName,
                        supportedType.getTypeSignature(),
                        DataTypes.DOUBLE.getTypeSignature()),
                    (signature, boundSignature) ->
                        new AverageAggregation(signature, boundSignature,
                            supportedType.id() != DataTypes.FLOAT.id() && supportedType.id() != DataTypes.DOUBLE.id())
                );
            }
        }
    }

    public static class AverageState implements Comparable<AverageState> {

        private double sum = 0;
        private long count = 0;
        private final KahanSummationForDouble kahanSummationForDouble = new KahanSummationForDouble();

        public Double value() {
            if (count > 0) {
                return sum / count;
            } else {
                return null;
            }
        }

        public void addNumber(double number, boolean isIntegral) {
            this.sum = isIntegral ? this.sum + number : kahanSummationForDouble.sum(this.sum, number);
            this.count++;
        }

        public void removeNumber(double number, boolean isIntegral) {
            this.sum = isIntegral ? this.sum - number : kahanSummationForDouble.sum(this.sum, -number);
            this.count--;
        }

        public void reduce(@Nonnull AverageState other, boolean isIntegral) {
            this.count += other.count;
            this.sum = isIntegral ? this.sum + other.sum : kahanSummationForDouble.sum(this.sum, other.sum);
        }

        @Override
        public int compareTo(AverageState o) {
            if (o == null) {
                return 1;
            } else {
                int compare = Double.compare(sum, o.sum);
                if (compare == 0) {
                    return Long.compare(count, o.count);
                }
                return compare;
            }
        }

        @Override
        public String toString() {
            return "sum: " + sum + " count: " + count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AverageState that = (AverageState) o;
            return Objects.equals(that.value(), value());
        }

        @Override
        public int hashCode() {
            return Objects.hash(value());
        }
    }

    public static class AverageStateType extends DataType<AverageState>
        implements FixedWidthType, Streamer<AverageState> {

        public static final int ID = 1024;
        private static final AverageStateType INSTANCE = new AverageStateType();
        private static final int AVERAGE_STATE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(AverageState.class);

        @Override
        public int id() {
            return ID;
        }

        @Override
        public Precedence precedence() {
            return Precedence.CUSTOM;
        }

        @Override
        public String getName() {
            return "average_state";
        }

        @Override
        public Streamer<AverageState> streamer() {
            return this;
        }

        @Override
        public AverageState sanitizeValue(Object value) {
            return (AverageState) value;
        }

        @Override
        public int compare(AverageState val1, AverageState val2) {
            if (val1 == null) return -1;
            return val1.compareTo(val2);
        }

        @Override
        public AverageState readValueFrom(StreamInput in) throws IOException {
            AverageState averageState = new AverageState();
            averageState.sum = in.readDouble();
            averageState.count = in.readVLong();
            return averageState;
        }

        @Override
        public void writeValueTo(StreamOutput out, AverageState v) throws IOException {
            out.writeDouble(v.sum);
            out.writeVLong(v.count);
        }

        @Override
        public int fixedSize() {
            return AVERAGE_STATE_SIZE;
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final boolean isIntegral;

    AverageAggregation(Signature signature, Signature boundSignature, boolean isIntegral) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.isIntegral = isIntegral;
    }

    @Override
    public AverageState iterate(RamAccounting ramAccounting,
                                MemoryManager memoryManager,
                                AverageState state,
                                Input... args) {
        if (state != null) {
            Number value = (Number) args[0].value();
            if (value != null) {
                state.addNumber(value.doubleValue(), isIntegral); // Mutates state.
            }
        }
        return state;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public AverageState removeFromAggregatedState(RamAccounting ramAccounting,
                                                  AverageState previousAggState,
                                                  Input[] stateToRemove) {
        if (previousAggState != null) {
            Number value = (Number) stateToRemove[0].value();
            if (value != null) {
                previousAggState.removeNumber(value.doubleValue(), isIntegral); // Mutates previousAggState.
            }
        }
        return previousAggState;
    }

    @Override
    public AverageState reduce(RamAccounting ramAccounting, AverageState state1, AverageState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.reduce(state2, isIntegral); // Mutates state1.
        return state1;
    }

    @Override
    public Double terminatePartial(RamAccounting ramAccounting, AverageState state) {
        return state.value();
    }

    @Nullable
    @Override
    public AverageState newState(RamAccounting ramAccounting,
                                 Version indexVersionCreated,
                                 Version minNodeInCluster,
                                 MemoryManager memoryManager) {
        ramAccounting.addBytes(AverageStateType.INSTANCE.fixedSize());
        return new AverageState();
    }

    @Override
    public DataType<?> partialType() {
        return AverageStateType.INSTANCE;
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
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(AverageStateType.INSTANCE.fixedSize());
                        return new AverageState();
                    },
                    (values, state) -> {
                        state.addNumber(values.nextValue(), true); // Mutates state.
                    }
                );
            case FloatType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(AverageStateType.INSTANCE.fixedSize());
                        return new AverageState();
                    },
                    (values, state) -> {
                        var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                        state.addNumber(value, false); // Mutates state.
                    }
                );
            case DoubleType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(AverageStateType.INSTANCE.fixedSize());
                        return new AverageState();
                    },
                    (values, state) -> {
                        var value = NumericUtils.sortableLongToDouble((values.nextValue()));
                        state.addNumber(value, false); // Mutates state.
                    }
                );
            default:
                return null;
        }
    }
}

