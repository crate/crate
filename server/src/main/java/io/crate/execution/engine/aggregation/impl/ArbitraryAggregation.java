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

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.common.MutableObject;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.Signature;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class ArbitraryAggregation extends AggregationFunction<Object, Object> {

    public static final String NAME = "arbitrary";

    public static void register(AggregationImplModule mod) {
        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    supportedType.getTypeSignature()),
                ArbitraryAggregation::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final SizeEstimator<Object> partialEstimator;

    ArbitraryAggregation(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        partialEstimator = SizeEstimatorFactory.create(partialType());
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

    @Nullable
    @Override
    public Object newState(RamAccounting ramAccounting,
                           Version indexVersionCreated,
                           Version minNodeInCluster,
                           MemoryManager memoryManager) {
        return null;
    }

    @Override
    public Object iterate(RamAccounting ramAccounting,
                          MemoryManager memoryManager,
                          Object state,
                          Input... args) {
        return reduce(ramAccounting, state, args[0].value());
    }

    @Override
    public Object reduce(RamAccounting ramAccounting, Object state1, Object state2) {
        if (state1 == null) {
            if (state2 != null) {
                // this case happens only once per aggregation so ram usage is only estimated once
                ramAccounting.addBytes(partialEstimator.estimateSize(state2));
            }
            return state2;
        }
        return state1;
    }

    @Override
    public Object terminatePartial(RamAccounting ramAccounting, Object state) {
        return state;
    }

    @Nullable
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
                return new ArbitraryNumericDocValueAggregator(
                    fieldTypes.get(0).name(),
                    dataType,
                    (values, state) -> {
                        if (!state.hasValue()) {
                            state.setValue(values.nextValue());
                        }
                    }
                );
            case FloatType.ID:
                return new ArbitraryNumericDocValueAggregator(
                    fieldTypes.get(0).name(),
                    dataType,
                    (values, state) -> {
                        if (!state.hasValue()) {
                            var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                            state.setValue(value);
                        }
                    }
                );
            case DoubleType.ID:
                return new ArbitraryNumericDocValueAggregator(
                    fieldTypes.get(0).name(),
                    dataType,
                    (values, state) -> {
                        if (!state.hasValue()) {
                            var value = NumericUtils.sortableLongToDouble(values.nextValue());
                            state.setValue(value);
                        }
                    }
                );
            case IpType.ID:
            case StringType.ID:
                return new BinaryDocValueAggregator<>(
                    fieldTypes.get(0).name(),
                    MutableObject::new,
                    (values, state) -> {
                        if (!state.hasValue()) {
                            state.setValue(values.nextValue().utf8ToString());
                        }
                    }
                ) {
                    @Nullable
                    @Override
                    public Object partialResult(RamAccounting ramAccounting, MutableObject state) {
                        if (state.hasValue()) {
                            return dataType.sanitizeValue(state.value());
                        } else {
                            return null;
                        }
                    }
                };
            default:
                return null;
        }
    }

    private static class ArbitraryNumericDocValueAggregator extends SortedNumericDocValueAggregator<MutableObject> {

        private final DataType<?> columnDataType;

        public ArbitraryNumericDocValueAggregator(
            String columnName,
            DataType<?> columnDataType,
            CheckedBiConsumer<SortedNumericDocValues, MutableObject, IOException> docValuesConsumer
        ) {
            super(columnName, MutableObject::new, docValuesConsumer);
            this.columnDataType = columnDataType;
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableObject state) {
            if (state.hasValue()) {
                return columnDataType.sanitizeValue(state.value());
            } else {
                return null;
            }
        }
    }
}
