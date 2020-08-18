/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.common.MutableFloat;
import io.crate.types.ByteType;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.mapper.MappedFieldType;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.common.MutableDouble;
import io.crate.common.MutableLong;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FixedWidthType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;

public abstract class MaximumAggregation extends AggregationFunction<Comparable, Comparable> {

    public static final String NAME = "max";

    public static void register(AggregationImplModule mod) {
        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            var fixedWidthType = supportedType instanceof FixedWidthType;
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    supportedType.getTypeSignature()),
                (signature, boundSignature) ->
                    fixedWidthType
                    ? new FixedMaximumAggregation(signature, boundSignature)
                    : new VariableMaximumAggregation(signature, boundSignature)
            );
        }
    }


    private static class LongMax extends SortedNumericDocValueAggregator<MutableLong> {

        private final DataType<?> partialType;

        public LongMax(String columnName,
                       DataType<?> partialType,
                       CheckedBiConsumer<SortedNumericDocValues, MutableLong, IOException> docValuesConsumer) {
            super(columnName, () -> new MutableLong(Long.MIN_VALUE), docValuesConsumer);
            this.partialType = partialType;
        }

        @Override
        public Object partialResult(MutableLong state) {
            if (state.hasValue()) {
                return partialType.sanitizeValue(state.value());
            } else {
                return null;
            }
        }
    }

    private static class DoubleMax extends SortedNumericDocValueAggregator<MutableDouble> {

        public DoubleMax(String columnName,
                         CheckedBiConsumer<SortedNumericDocValues, MutableDouble, IOException> docValuesConsumer) {
            super(columnName, () -> new MutableDouble(Double.MIN_VALUE), docValuesConsumer);
        }

        @Override
        public Object partialResult(MutableDouble state) {
            if (state.hasValue()) {
                return state.value();
            } else {
                return null;
            }
        }
    }

    private static class FloatMax extends SortedNumericDocValueAggregator<MutableFloat> {

        public FloatMax(String columnName,
                        CheckedBiConsumer<SortedNumericDocValues, MutableFloat, IOException> docValuesConsumer) {
            super(columnName, () -> new MutableFloat(Float.MIN_VALUE), docValuesConsumer);
        }

        @Override
        public Object partialResult(MutableFloat state) {
            if (state.hasValue()) {
                return state.value();
            } else {
                return null;
            }
        }
    }

    private static class FixedMaximumAggregation extends MaximumAggregation {

        private final int size;

        public FixedMaximumAggregation(Signature signature, Signature boundSignature) {
            super(signature, boundSignature);
            size = ((FixedWidthType) partialType()).fixedSize();
        }

        @Override
        public DocValueAggregator<?> getDocValueAggregator(List<DataType<?>> argumentTypes,
                                                           List<MappedFieldType> fieldTypes) {
            var dataType = argumentTypes.get(0);
            switch (dataType.id()) {
                case ByteType.ID:
                case ShortType.ID:
                case IntegerType.ID:
                case LongType.ID:
                case TimestampType.ID_WITH_TZ:
                case TimestampType.ID_WITHOUT_TZ:
                    return new LongMax(
                        fieldTypes.get(0).name(),
                        dataType,
                        (values, state) -> {
                            long value = values.nextValue();
                            if (value > state.value()) {
                                state.setValue(value);
                            }
                        }
                    );
                case FloatType.ID:
                    return new FloatMax(
                        fieldTypes.get(0).name(),
                        (values, state) -> {
                            float value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                            if (value > state.value()) {
                                state.setValue(value);
                            }
                        }
                    );
                case DoubleType.ID:
                    return new DoubleMax(
                        fieldTypes.get(0).name(),
                        (values, state) -> {
                            double value = NumericUtils.sortableLongToDouble(values.nextValue());
                            if (value > state.value()) {
                                state.setValue(value);
                            }
                        }
                    );
                default:
                    return null;
            }
        }

        @Nullable
        @Override
        public Comparable newState(RamAccounting ramAccounting,
                                   Version indexVersionCreated,
                                   Version minNodeInCluster,
                                   MemoryManager memoryManager) {
            ramAccounting.addBytes(size);
            return null;
        }

        @Override
        public Comparable reduce(RamAccounting ramAccounting, Comparable state1, Comparable state2) {
            if (state1 == null) {
                return state2;
            }
            if (state2 == null) {
                return state1;
            }
            if (state1.compareTo(state2) < 0) {
                return state2;
            }
            return state1;
        }
    }

    private static class VariableMaximumAggregation extends MaximumAggregation {

        private final SizeEstimator<Object> estimator;

        VariableMaximumAggregation(Signature signature, Signature boundSignature) {
            super(signature, boundSignature);
            estimator = SizeEstimatorFactory.create(partialType());
        }

        @Nullable
        @Override
        public Comparable newState(RamAccounting ramAccounting,
                                   Version indexVersionCreated,
                                   Version minNodeInCluster,
                                   MemoryManager memoryManager) {
            return null;
        }

        @Override
        public Comparable reduce(RamAccounting ramAccounting, Comparable state1, Comparable state2) {
            if (state1 == null) {
                if (state2 != null) {
                    ramAccounting.addBytes(estimator.estimateSize(state2));
                }
                return state2;
            }
            if (state2 == null) {
                return state1;
            }
            if (state1.compareTo(state2) < 0) {
                ramAccounting.addBytes(estimator.estimateSizeDelta(state1, state2));
                return state2;
            }
            return state1;
        }
    }

    private final Signature signature;
    private final Signature boundSignature;

    private MaximumAggregation(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public DataType<?> partialType() {
        return boundSignature.getReturnType().createType();
    }

    @Override
    public Comparable iterate(RamAccounting ramAccounting,
                              MemoryManager memoryManager,
                              Comparable state,
                              Input... args) throws CircuitBreakingException {
        Object value = args[0].value();
        return reduce(ramAccounting, state, (Comparable) value);
    }

    @Override
    public Comparable terminatePartial(RamAccounting ramAccounting, Comparable state) {
        return state;
    }
}
