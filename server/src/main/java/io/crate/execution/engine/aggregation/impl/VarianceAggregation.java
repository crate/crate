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

import javax.annotation.Nullable;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.execution.engine.aggregation.statistics.Variance;
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

public class VarianceAggregation extends AggregationFunction<Variance, Double> {

    public static final String NAME = "variance";

    static {
        DataTypes.register(VarianceStateType.ID, in -> VarianceStateType.INSTANCE);
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
                VarianceAggregation::new
            );
        }
    }

    public static class VarianceStateType extends DataType<Variance> implements Streamer<Variance>, FixedWidthType {

        public static final VarianceStateType INSTANCE = new VarianceStateType();
        public static final int ID = 2048;

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
            return "variance_state";
        }

        @Override
        public Streamer<Variance> streamer() {
            return this;
        }

        @Override
        public Variance sanitizeValue(Object value) {
            return (Variance) value;
        }

        @Override
        public int compare(Variance val1, Variance val2) {
            return val1.compareTo(val2);
        }

        @Override
        public Variance readValueFrom(StreamInput in) throws IOException {
            return new Variance(in);
        }

        @Override
        public void writeValueTo(StreamOutput out, Variance v) throws IOException {
            v.writeTo(out);
        }

        @Override
        public int fixedSize() {
            return 56;
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    public VarianceAggregation(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }



    @Nullable
    @Override
    public Variance newState(RamAccounting ramAccounting,
                             Version indexVersionCreated,
                             Version minNodeInCluster,
                             MemoryManager memoryManager) {
        ramAccounting.addBytes(VarianceStateType.INSTANCE.fixedSize());
        return new Variance();
    }

    @Override
    public Variance iterate(RamAccounting ramAccounting,
                            MemoryManager memoryManager,
                            Variance state,
                            Input... args) throws CircuitBreakingException {
        if (state != null) {
            Number value = (Number) args[0].value();
            if (value != null) {
                state.increment(value.doubleValue());
            }
        }
        return state;
    }

    @Override
    public Variance reduce(RamAccounting ramAccounting, Variance state1, Variance state2) {
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
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public Variance removeFromAggregatedState(RamAccounting ramAccounting,
                                              Variance previousAggState,
                                              Input[] stateToRemove) {
        if (previousAggState != null) {
            Number value = (Number) stateToRemove[0].value();
            if (value != null) {
                previousAggState.decrement(value.doubleValue());
            }
        }
        return previousAggState;
    }

    @Override
    public Double terminatePartial(RamAccounting ramAccounting, Variance state) {
        double result = state.result();
        return Double.isNaN(result) ? null : result;
    }

    @Override
    public DataType<?> partialType() {
        return VarianceStateType.INSTANCE;
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
                        ramAccounting.addBytes(VarianceStateType.INSTANCE.fixedSize());
                        return new Variance();
                    },
                    (values, state) -> state.increment(values.nextValue())
                );
            case FloatType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(VarianceStateType.INSTANCE.fixedSize());
                        return new Variance();
                    },
                    (values, state) -> {
                        var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                        state.increment(value);
                    }
                );
            case DoubleType.ID:
                return new SortedNumericDocValueAggregator<>(
                    reference.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(VarianceStateType.INSTANCE.fixedSize());
                        return new Variance();
                    },
                    (values, state) -> {
                        var value = NumericUtils.sortableLongToDouble((values.nextValue()));
                        state.increment(value);
                    }
                );
            default:
                return null;
        }
    }
}
