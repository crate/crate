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
import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nullable;

import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;
import io.crate.execution.engine.aggregation.impl.util.OverflowAwareMutableLong;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.crate.breaker.RamAccounting;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
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
import io.crate.types.NumericType;
import io.crate.types.ShortType;

public class NumericSumAggregation extends AggregationFunction<BigDecimal, BigDecimal> {

    public static final String NAME = "sum";
    public static final Signature SIGNATURE = Signature.aggregate(
        NAME,
        DataTypes.NUMERIC.getTypeSignature(),
        DataTypes.NUMERIC.getTypeSignature()
    );
    private static final long INIT_BIG_DECIMAL_SIZE = NumericType.size(BigDecimal.ZERO);

    public static void register(AggregationImplModule mod) {
        mod.register(SIGNATURE, NumericSumAggregation::new);
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<BigDecimal> returnType;

    @VisibleForTesting
    private NumericSumAggregation(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        // We want to preserve the scale and precision from the
        // numeric argument type for the return type. So we use
        // the incoming numeric type as return type instead of
        // the return type from the signature `sum(count::numeric(16, 2))`
        // should return the type `numeric(16, 2)` not `numeric`
        var argumentType = boundSignature.getArgumentDataTypes().get(0);
        assert argumentType.id() == DataTypes.NUMERIC.id();
        //noinspection unchecked
        this.returnType = (DataType<BigDecimal>) argumentType;
    }

    @Nullable
    @Override
    public BigDecimal newState(RamAccounting ramAccounting,
                               Version indexVersionCreated,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
        ramAccounting.addBytes(INIT_BIG_DECIMAL_SIZE);
        return null;
    }

    @Override
    public BigDecimal iterate(RamAccounting ramAccounting,
                              MemoryManager memoryManager,
                              BigDecimal state,
                              Input[] args) throws CircuitBreakingException {
        BigDecimal value = returnType.implicitCast(args[0].value());
        if (value != null) {
            if (state != null) {
                var newState = state.add(value);
                ramAccounting.addBytes(NumericType.sizeDiff(newState, state));
                state = newState;
            } else {
                state = value;
            }
        }
        return state;
    }

    @Override
    public BigDecimal reduce(RamAccounting ramAccounting,
                             BigDecimal state1,
                             BigDecimal state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        return state1.add(state2);
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, BigDecimal state) {
        if (state != null) {
            ramAccounting.addBytes(NumericType.size(state));
        }
        return state;
    }

    @Override
    public DataType<?> partialType() {
        return returnType;
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
    public BigDecimal removeFromAggregatedState(RamAccounting ramAccounting,
                                                BigDecimal previousAggState,
                                                Input[] stateToRemove) {
        BigDecimal value = returnType.implicitCast(stateToRemove[0].value());
        if (value != null) {
            if (previousAggState != null) {
                return previousAggState.subtract(value);
            }
        }
        return previousAggState;
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
        return switch (reference.valueType().id()) {
            case ByteType.ID, ShortType.ID, IntegerType.ID, LongType.ID ->
                new SumLong(returnType, reference.column().fqn());
            case FloatType.ID -> new SumFloat(returnType, reference.column().fqn());
            case DoubleType.ID -> new SumDouble(returnType, reference.column().fqn());
            default -> null;
        };
    }

    static class SumLong implements DocValueAggregator<OverflowAwareMutableLong> {

        private final DataType<BigDecimal> returnType;
        private final String columnName;
        private SortedNumericDocValues values;

        SumLong(DataType<BigDecimal> returnType, String columnName) {
            this.returnType = returnType;
            this.columnName = columnName;
        }

        @Override
        public OverflowAwareMutableLong initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(INIT_BIG_DECIMAL_SIZE);
            return new OverflowAwareMutableLong(0L);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting,
                          int doc,
                          OverflowAwareMutableLong state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var prevState = state.value();
                state.add(values.nextValue());
                ramAccounting.addBytes(NumericType.sizeDiff(state.value(), prevState));
            }
        }

        @Override
        public BigDecimal partialResult(RamAccounting ramAccounting,
                                        OverflowAwareMutableLong state) {
            if (state.hasValue()) {
                return returnType.implicitCast(state.value());
            } else {
                return null;
            }
        }
    }

    static class SumDouble implements DocValueAggregator<BigDecimalValueWrapper> {

        private final DataType<BigDecimal> returnType;
        private final String columnName;
        private SortedNumericDocValues values;

        SumDouble(DataType<BigDecimal> returnType, String columnName) {
            this.returnType = returnType;
            this.columnName = columnName;
        }

        @Override
        public BigDecimalValueWrapper initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(INIT_BIG_DECIMAL_SIZE);
            return new BigDecimalValueWrapper(BigDecimal.ZERO);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting,
                          int doc,
                          BigDecimalValueWrapper state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var prevState = state.value();

                var fieldValue = returnType.implicitCast(
                    NumericUtils.sortableLongToDouble(values.nextValue()));
                state.setValue(state.value().add(fieldValue));

                ramAccounting.addBytes(NumericType.sizeDiff(state.value(), prevState));
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, BigDecimalValueWrapper state) {
            if (state.hasValue()) {
                return returnType.implicitCast(state.value());
            } else {
                return null;
            }
        }
    }

    static class SumFloat implements DocValueAggregator<BigDecimalValueWrapper> {

        private final DataType<BigDecimal> returnType;
        private final String columnName;
        private SortedNumericDocValues values;

        SumFloat(DataType<BigDecimal> returnType, String columnName) {
            this.returnType = returnType;
            this.columnName = columnName;
        }

        @Override
        public BigDecimalValueWrapper initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(INIT_BIG_DECIMAL_SIZE);
            return new BigDecimalValueWrapper(BigDecimal.ZERO);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting,
                          int doc,
                          BigDecimalValueWrapper state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var prevState = state.value();

                var fieldValue = returnType.implicitCast(
                    NumericUtils.sortableIntToFloat((int) values.nextValue()));
                state.setValue(state.value().add(fieldValue));

                ramAccounting.addBytes(NumericType.sizeDiff(state.value(), prevState));
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, BigDecimalValueWrapper state) {
            if (state.hasValue()) {
                return returnType.implicitCast(state.value());
            } else {
                return null;
            }
        }
    }
}
