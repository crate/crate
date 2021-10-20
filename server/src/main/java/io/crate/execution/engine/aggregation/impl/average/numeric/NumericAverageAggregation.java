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

package io.crate.execution.engine.aggregation.impl.average.numeric;

import io.crate.breaker.RamAccounting;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.AggregationImplModule;
import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;
import io.crate.execution.engine.aggregation.impl.util.OverflowAwareMutableLong;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbols;
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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import static io.crate.execution.engine.aggregation.impl.average.AverageAggregation.NAMES;
import static io.crate.execution.engine.aggregation.impl.average.numeric.NumericAverageStateType.INIT_SIZE;

public class NumericAverageAggregation extends AggregationFunction<NumericAverageState, BigDecimal> {

    static {
        DataTypes.register(NumericAverageStateType.ID, in -> NumericAverageStateType.INSTANCE);
    }

    public static void register(AggregationImplModule mod) {
        for (var functionName : NAMES) {
            mod.register(
                Signature.aggregate(
                    functionName,
                    DataTypes.NUMERIC.getTypeSignature(),
                    DataTypes.NUMERIC.getTypeSignature()),
                NumericAverageAggregation::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<BigDecimal> returnType;

    @VisibleForTesting
    private NumericAverageAggregation(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        // We want to preserve the scale and precision from the
        // numeric argument type for the return type. So we use
        // the incoming numeric type as return type instead of
        // the return type from the signature `avg(count::numeric(16, 2))`
        // should return the type `numeric(16, 2)` not `numeric`
        var argumentType = boundSignature.getArgumentDataTypes().get(0);
        assert argumentType.id() == DataTypes.NUMERIC.id();
        //noinspection unchecked
        this.returnType = (DataType<BigDecimal>) argumentType;
    }

    @Nullable
    @Override
    public NumericAverageState newState(RamAccounting ramAccounting,
                               Version indexVersionCreated,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
        ramAccounting.addBytes(INIT_SIZE);
        return new NumericAverageState(new BigDecimalValueWrapper(BigDecimal.ZERO), 0L);
    }

    @Override
    public NumericAverageState iterate(RamAccounting ramAccounting,
                              MemoryManager memoryManager,
                              NumericAverageState state,
                              Input[] args) throws CircuitBreakingException {
        if (state != null) {
            BigDecimal value = returnType.implicitCast(args[0].value());
            if (value != null) {
                BigDecimal newValue = state.sum.value().add(value);
                ramAccounting.addBytes(NumericType.sizeDiff(newValue, state.sum.value()));
                ((BigDecimalValueWrapper) state.sum).setValue(newValue);
                state.count++;
            }
        }
        return state;
    }

    @Override
    public NumericAverageState reduce(RamAccounting ramAccounting,
                               NumericAverageState state1,
                               NumericAverageState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        BigDecimal newValue = state1.sum.value().add(state2.sum.value());
        ramAccounting.addBytes(NumericType.sizeDiff(newValue, state1.sum.value()));
        ((BigDecimalValueWrapper) state1.sum).setValue(newValue);
        state1.count += state2.count;
        return state1;
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, NumericAverageState state) {

        return state.value();
    }

    @Override
    public DataType<?> partialType() {
        return NumericAverageStateType.INSTANCE;
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
    public NumericAverageState removeFromAggregatedState(RamAccounting ramAccounting,
                                                         NumericAverageState previousAggState,
                                                         Input[] stateToRemove) {
        if (previousAggState != null) {
            BigDecimal value = returnType.implicitCast(stateToRemove[0].value());
            if (value != null) {
                BigDecimal newValue = previousAggState.sum.value().subtract(value);
                ramAccounting.addBytes(NumericType.sizeDiff(newValue, previousAggState.sum.value()));
                previousAggState.count--;
                ((BigDecimalValueWrapper) previousAggState.sum).setValue(newValue);
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
        var argumentTypes = Symbols.typeView(aggregationReferences);
        return switch (argumentTypes.get(0).id()) {
            case ByteType.ID, ShortType.ID, IntegerType.ID, LongType.ID ->
                new AvgLong(returnType, reference.column().fqn());
            case FloatType.ID -> new AvgFloat(returnType, reference.column().fqn());
            case DoubleType.ID -> new AvgDouble(returnType, reference.column().fqn());
            default -> null;
        };
    }

    static class AvgLong implements DocValueAggregator<NumericAverageState> {

        private final DataType<BigDecimal> returnType;
        private final String columnName;
        private SortedNumericDocValues values;

        AvgLong(DataType<BigDecimal> returnType, String columnName) {
            this.returnType = returnType;
            this.columnName = columnName;
        }

        @Override
        public NumericAverageState initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(INIT_SIZE);
            return new NumericAverageState(new OverflowAwareMutableLong(0L), 0L);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting,
                          int doc,
                          NumericAverageState state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (state != null) {
                    ((OverflowAwareMutableLong) state.sum).add(values.nextValue());
                    state.count++;
                }
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, NumericAverageState state) {
            return state;
        }
    }

    static class AvgDouble implements DocValueAggregator<NumericAverageState> {

        private final DataType<BigDecimal> returnType;
        private final String columnName;
        private SortedNumericDocValues values;

        AvgDouble(DataType<BigDecimal> returnType, String columnName) {
            this.returnType = returnType;
            this.columnName = columnName;
        }

        @Override
        public NumericAverageState initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(INIT_SIZE);
            return new NumericAverageState(new BigDecimalValueWrapper(BigDecimal.ZERO), 0L);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting,
                          int doc,
                          NumericAverageState state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (state != null) {
                    BigDecimal value = returnType.implicitCast(NumericUtils.sortableLongToDouble(values.nextValue()));
                    BigDecimal newValue = state.sum.value().add(value);
                    ramAccounting.addBytes(NumericType.sizeDiff(newValue, state.sum.value()));
                    ((BigDecimalValueWrapper) state.sum).setValue(newValue);
                    state.count++;

                }
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, NumericAverageState state) {
            return state;
        }
    }

    static class AvgFloat implements DocValueAggregator<NumericAverageState> {

        private final DataType<BigDecimal> returnType;
        private final String columnName;
        private SortedNumericDocValues values;

        AvgFloat(DataType<BigDecimal> returnType, String columnName) {
            this.returnType = returnType;
            this.columnName = columnName;
        }

        @Override
        public NumericAverageState initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(INIT_SIZE);
            return new NumericAverageState(new BigDecimalValueWrapper(BigDecimal.ZERO), 0L);
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting,
                          int doc,
                          NumericAverageState state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (state != null) {
                    BigDecimal value = returnType.implicitCast(NumericUtils.sortableIntToFloat((int) values.nextValue()));
                    BigDecimal newValue = state.sum.value().add(value);
                    ramAccounting.addBytes(NumericType.sizeDiff(newValue, state.sum.value()));
                    ((BigDecimalValueWrapper) state.sum).setValue(newValue);
                    state.count++;
                }
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, NumericAverageState state) {
            return state;
        }
    }

}
