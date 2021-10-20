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
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import javax.annotation.Nullable;

import io.crate.execution.engine.aggregation.impl.util.KahanSummationForDouble;
import io.crate.execution.engine.aggregation.impl.util.KahanSummationForFloat;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.crate.breaker.RamAccounting;
import io.crate.common.MutableDouble;
import io.crate.common.MutableFloat;
import io.crate.common.MutableLong;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;

public class SumAggregation<T extends Number> extends AggregationFunction<T, T> {

    public static final String NAME = "sum";

    public static void register(AggregationImplModule mod) {
        BinaryOperator<Long> add = Math::addExact;
        BinaryOperator<Long> sub = Math::subtractExact;

        mod.register(
            Signature.aggregate(
                NAME,
                DataTypes.FLOAT.getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()
            ),
            getSumAggregationForFloatFactory()
        );
        mod.register(
            Signature.aggregate(
                NAME,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            getSumAggregationForDoubleFactory()
        );

        for (var supportedType : List.of(DataTypes.BYTE, DataTypes.SHORT, DataTypes.INTEGER, DataTypes.LONG)) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()),
                (signature, boundSignature) ->
                    new SumAggregation<>(DataTypes.LONG, add, sub, signature, boundSignature)
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final BinaryOperator<T> addition;
    private final BinaryOperator<T> subtraction;
    private final DataType<T> returnType;
    private final int bytesSize;

    @VisibleForTesting
    private SumAggregation(final DataType<T> returnType,
                           final BinaryOperator<T> addition,
                           final BinaryOperator<T> subtraction,
                           Signature signature,
                           Signature boundSignature) {
        this.addition = addition;
        this.subtraction = subtraction;
        this.returnType = returnType;

        if (returnType == DataTypes.FLOAT) {
            bytesSize = DataTypes.FLOAT.fixedSize();
        } else if (returnType == DataTypes.DOUBLE) {
            bytesSize = DataTypes.DOUBLE.fixedSize();
        } else {
            bytesSize = DataTypes.LONG.fixedSize();
        }

        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Nullable
    @Override
    public T newState(RamAccounting ramAccounting,
                      Version indexVersionCreated,
                      Version minNodeInCluster,
                      MemoryManager memoryManager) {
        ramAccounting.addBytes(bytesSize);
        return null;
    }

    @Override
    public T iterate(RamAccounting ramAccounting,
                     MemoryManager memoryManager,
                     T state,
                     Input[] args) throws CircuitBreakingException {
        return reduce(ramAccounting, state, returnType.sanitizeValue(args[0].value()));
    }

    @Override
    public T reduce(RamAccounting ramAccounting, T state1, T state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        return addition.apply(state1, state2);
    }

    @Override
    public T terminatePartial(RamAccounting ramAccounting, T state) {
        return state;
    }

    @Override
    public DataType<?> partialType() {
        return boundSignature.getReturnType().createType();
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
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public T removeFromAggregatedState(RamAccounting ramAccounting, T previousAggState, Input[] stateToRemove) {
        return subtraction.apply(previousAggState, returnType.sanitizeValue(stateToRemove[0].value()));
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
                return new SumLong(reference.column().fqn());

            case FloatType.ID:
                return new SumFloat(reference.column().fqn());
            case DoubleType.ID:
                return new SumDouble(reference.column().fqn());

            default:
                return null;
        }
    }

    private static BiFunction<Signature, Signature, FunctionImplementation> getSumAggregationForDoubleFactory() {
        return (signature, boundSignature) -> {
            var kahanSummation = new KahanSummationForDouble();
            return new SumAggregation<>(
                DataTypes.DOUBLE,
                kahanSummation::sum,
                (n1, n2) -> n1 - n2,
                signature,
                boundSignature
            );
        };
    }

    private static BiFunction<Signature, Signature, FunctionImplementation> getSumAggregationForFloatFactory() {
        return (signature, boundSignature) -> {
            var kahanSummation = new KahanSummationForFloat();
            return new SumAggregation<>(
                DataTypes.FLOAT,
                kahanSummation::sum,
                (n1, n2) -> n1 - n2,
                signature,
                boundSignature
            );
        };
    }

    @VisibleForTesting
    public static class SumLong implements DocValueAggregator<MutableLong> {

        private final String columnName;
        private SortedNumericDocValues values;

        SumLong(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public MutableLong initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(DataTypes.LONG.fixedSize());
            return new MutableLong(0L);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableLong state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                state.setValue(Math.addExact(state.value(), values.nextValue()));
            }
        }

        @Override
        public Long partialResult(RamAccounting ramAccounting, MutableLong state) {
            return state.hasValue() ? state.value() : null;
        }
    }

    static class SumDouble implements DocValueAggregator<MutableDouble> {

        private final String columnName;
        private SortedNumericDocValues values;
        private final KahanSummationForDouble kahanSummation = new KahanSummationForDouble();

        SumDouble(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public MutableDouble initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(DataTypes.DOUBLE.fixedSize());
            return new MutableDouble(.0d);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableDouble state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var value = kahanSummation.sum(
                    state.value(),
                    NumericUtils.sortableLongToDouble(values.nextValue())
                );
                state.setValue(value);
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableDouble state) {
            return state.hasValue() ? state.value() : null;
        }
    }

    static class SumFloat implements DocValueAggregator<MutableFloat> {

        private final String columnName;
        private SortedNumericDocValues values;
        private final KahanSummationForFloat kahanSummation = new KahanSummationForFloat();

        SumFloat(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public MutableFloat initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(DataTypes.FLOAT.fixedSize());
            return new MutableFloat(.0f);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableFloat state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var value = kahanSummation.sum(
                    state.value(),
                    NumericUtils.sortableIntToFloat((int) values.nextValue())
                );
                state.setValue(value);
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableFloat state) {
            return state.hasValue() ? state.value() : null;
        }
    }
}
