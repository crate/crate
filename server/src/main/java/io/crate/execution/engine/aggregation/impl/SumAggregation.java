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
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.mapper.MappedFieldType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.BinaryOperator;

public class SumAggregation<T extends Number> extends AggregationFunction<T, T> {

    public static final String NAME = "sum";

    public static void register(AggregationImplModule mod) {
        BinaryOperator<Long> add = Math::addExact;
        BinaryOperator<Long> sub = Math::subtractExact;

        mod.register(
            Signature.aggregate(
                NAME,
                DataTypes.FLOAT.getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()),
            (signature, boundSignature) ->
                new SumAggregation<>(DataTypes.FLOAT, Float::sum, (n1, n2) -> n1 - n2, signature, boundSignature)
        );
        mod.register(
            Signature.aggregate(
                NAME,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()),
            (signature, boundSignature) ->
                new SumAggregation<>(DataTypes.DOUBLE, Double::sum, (n1, n2) -> n1 - n2, signature, boundSignature)
        );

        for (var supportedType : List.of(DataTypes.BYTE, DataTypes.SHORT, DataTypes.INTEGER, DataTypes.LONG)) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()),
                (signature, boundSignature) ->
                    new SumAggregation<>(supportedType, DataTypes.LONG, add, sub, signature, boundSignature)
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
        this(returnType, returnType, addition, subtraction, signature, boundSignature);
    }

    private SumAggregation(final DataType<?> inputType,
                           final DataType<T> returnType,
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
        this.boundSignature = signature;
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
    public T iterate(RamAccounting ramAccounting, MemoryManager memoryManager, T state, Input[] args) throws CircuitBreakingException {
        return reduce(ramAccounting, state, returnType.value(args[0].value()));
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
        return subtraction.apply(previousAggState, returnType.value(stateToRemove[0].value()));
    }

    @Override
    public DocValueAggregator<?> getDocValueAggregator(List<DataType<?>> argumentTypes, List<MappedFieldType> fieldTypes) {
        switch (argumentTypes.get(0).id()) {
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
                return new SumLong(fieldTypes.get(0).name());

            case FloatType.ID:
            case DoubleType.ID:
                return new SumDouble(fieldTypes.get(0).name());

            default:
                return null;
        }
    }

    static class SumLongState {
        private long sum = 0L;
        private boolean hadValue = false;
    }

    static class SumLong implements DocValueAggregator<SumLongState> {

        private final String columnName;
        private SortedNumericDocValues values;

        SumLong(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public SumLongState initialState() {
            return new SumLongState();
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(SumLongState state, int doc) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                state.sum = Math.addExact(state.sum, values.nextValue());
                state.hadValue = true;
            }
        }

        @Override
        public Long partialResult(SumLongState state) {
            return state.hadValue ? state.sum : null;
        }
    }

    static class SumDoubleState {

        private double sum = 0.0;
        private boolean hadValue = false;
    }

    static class SumDouble implements DocValueAggregator<SumDoubleState> {

        private final String columnName;
        private SortedNumericDocValues values;

        SumDouble(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public SumDoubleState initialState() {
            return new SumDoubleState();
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(SumDoubleState state, int doc) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                state.sum += NumericUtils.sortableLongToDouble(values.nextValue());
                state.hadValue = true;
            }
        }

        @Override
        public Object partialResult(SumDoubleState state) {
            return state.hadValue ? state.sum : null;
        }
    }
}
