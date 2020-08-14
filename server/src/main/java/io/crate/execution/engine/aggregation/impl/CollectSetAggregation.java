/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
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
import io.crate.types.UncheckedObjectType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectSetAggregation extends AggregationFunction<Map<Object, Object>, List<Object>> {

    /**
     * Used to signal there is a value for a key in order to simulate {@link java.util.HashSet#add(Object)} semantics
     * using a plain {@link HashMap}.
     */
    private static final Object PRESENT = null;

    public static final String NAME = "collect_set";

    public static void register(AggregationImplModule mod) {
        for (DataType<?> supportedType : DataTypes.PRIMITIVE_TYPES) {
            var returnType = new ArrayType<>(supportedType);
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    returnType.getTypeSignature()
                ),
                CollectSetAggregation::new
            );
        }
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<?> partialReturnType;
    private final SizeEstimator<Object> innerTypeEstimator;

    private CollectSetAggregation(Signature signature, Signature boundSignature) {
        this.innerTypeEstimator = SizeEstimatorFactory.create(
            ((ArrayType<?>) boundSignature.getReturnType().createType()).innerType()
        );
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.partialReturnType = UncheckedObjectType.INSTANCE;
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
    public AggregationFunction<Map<Object, Long>, List<Object>> optimizeForExecutionAsWindowFunction() {
        return new RemovableCumulativeCollectSet(signature, boundSignature);
    }

    @Override
    public Map<Object, Object> iterate(RamAccounting ramAccounting,
                                       MemoryManager memoryManager,
                                       Map<Object, Object> state,
                                       Input... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (value == null) {
            return state;
        }
        if (state.put(value, PRESENT) == null) {
            ramAccounting.addBytes(
                // values size + 32 bytes for entry, 4 bytes for increased capacity
                RamUsageEstimator.alignObjectSize(innerTypeEstimator.estimateSize(value) + 36L)
            );
        }
        return state;
    }

    @Nullable
    @Override
    public Map<Object, Object> newState(RamAccounting ramAccounting,
                                        Version indexVersionCreated,
                                        Version minNodeInCluster,
                                        MemoryManager memoryManager) {
        ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(64L)); // overhead for HashMap: 32 * 0 + 16 * 4 bytes
        return new HashMap<>();
    }

    @Override
    public DataType<?> partialType() {
        return partialReturnType;
    }

    @Override
    public Map<Object, Object> reduce(RamAccounting ramAccounting,
                                      Map<Object, Object> state1,
                                      Map<Object, Object> state2) {
        for (Object newValue : state2.keySet()) {
            if (state1.put(newValue, PRESENT) == null) {
                ramAccounting.addBytes(
                    // value size + 32 bytes for entry + 4 bytes for increased capacity
                    RamUsageEstimator.alignObjectSize(innerTypeEstimator.estimateSize(newValue) + 36L)
                );
            }
        }
        return state1;
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, Map<Object, Object> state) {
        return new ArrayList<>(state.keySet());
    }

    @Override
    public boolean isRemovableCumulative() {
        return false;
    }

    /**
     * collect_set implementation that is removable cumulative. It tracks the number of occurrences for every key it
     * sees in order to be able to only remove a value from the aggregated state when it's occurrence count is 1.
     *
     * eg. for a window definition of CURRENT ROW -> UNBOUNDED FOLLOWING for {1, 2, 2, 2, 3 } the window frames and
     * corresponding collect_set outputs are:
     *  {1, 2, 2, 2, 3}  - [1, 2, 3]
     *  {2, 2, 2, 3}     - [2, 3]
     *  {2, 2, 3}        - [2, 3]
     *  {2, 3}           - [2, 3]
     *  {3}              - [3]
     */
    private static class RemovableCumulativeCollectSet extends AggregationFunction<Map<Object, Long>, List<Object>> {

        private final SizeEstimator<Object> innerTypeEstimator;

        private final Signature signature;
        private final Signature boundSignature;
        private final DataType<?> partialType;

        RemovableCumulativeCollectSet(Signature signature, Signature boundSignature) {
            this.innerTypeEstimator = SizeEstimatorFactory.create(
                ((ArrayType<?>) boundSignature.getReturnType().createType()).innerType()
            );
            this.signature = signature;
            this.boundSignature = boundSignature;
            this.partialType = UncheckedObjectType.INSTANCE;
        }

        @Nullable
        @Override
        public Map<Object, Long> newState(RamAccounting ramAccounting,
                                          Version indexVersionCreated,
                                          Version minNodeInCluster,
                                          MemoryManager memoryManager) {
            ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(64L)); // overhead for HashMap: 32 * 0 + 16 * 4 bytes
            return new HashMap<>();
        }

        @Override
        public Map<Object, Long> iterate(RamAccounting ramAccounting,
                                         MemoryManager memoryManager, Map<Object, Long> state,
                                         Input... args) throws CircuitBreakingException {
            Object value = args[0].value();
            if (value == null) {
                return state;
            }
            upsertOccurrenceForValue(state, value, 1, ramAccounting, innerTypeEstimator);
            return state;
        }

        private static void upsertOccurrenceForValue(final Map<Object, Long> state,
                                                     final Object value,
                                                     final long occurrenceIncrement,
                                                     final RamAccounting ramAccountingContext,
                                                     final SizeEstimator<Object> innerTypeEstimator) {
            state.compute(value, (k, v) -> {
                if (v == null) {
                    ramAccountingContext.addBytes(
                        // values size + 32 bytes for entry, 4 bytes for increased capacity, 8 bytes for the new array
                        // instance and 4 for the occurrence count we store
                        RamUsageEstimator.alignObjectSize(innerTypeEstimator.estimateSize(value) + 48L)
                    );
                    return occurrenceIncrement;
                } else {
                    return v + occurrenceIncrement;
                }
            });
        }

        @Override
        public boolean isRemovableCumulative() {
            return true;
        }

        @Override
        public Map<Object, Long> removeFromAggregatedState(RamAccounting ramAccounting,
                                                           Map<Object, Long> previousAggState,
                                                           Input[] stateToRemove) {
            Object value = stateToRemove[0].value();
            if (value == null) {
                return previousAggState;
            }
            Long numTimesValueSeen = previousAggState.get(value);
            if (numTimesValueSeen == null) {
                return previousAggState;
            }
            if (numTimesValueSeen == 1) {
                previousAggState.remove(value);
                ramAccounting.addBytes(
                    // we initially accounted for values size + 32 bytes for entry, 4 bytes for increased capacity
                    // and 12 bytes for the array container and the int value it stored
                    - RamUsageEstimator.alignObjectSize(innerTypeEstimator.estimateSize(value) + 48L)
                );
            } else {
                previousAggState.put(value, numTimesValueSeen - 1);
            }
            return previousAggState;
        }

        @Override
        public Map<Object, Long> reduce(RamAccounting ramAccounting,
                                        Map<Object, Long> state1,
                                        Map<Object, Long> state2) {
            for (Map.Entry<Object, Long> state2Entry : state2.entrySet()) {
                upsertOccurrenceForValue(
                    state1,
                    state2Entry.getKey(),
                    state2Entry.getValue(),
                    ramAccounting,
                    innerTypeEstimator
                );
            }
            return state1;
        }

        @Override
        public List<Object> terminatePartial(RamAccounting ramAccounting, Map<Object, Long> state) {
            return new ArrayList<>(state.keySet());
        }

        @Override
        public DataType<?> partialType() {
            return partialType;
        }

        @Override
        public Signature signature() {
            return signature;
        }

        @Override
        public Signature boundSignature() {
            return boundSignature;
        }
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
                return new LongCollectSetNumericDocValueAggregator(
                    fieldTypes.get(0).name(),
                    dataType,
                    innerTypeEstimator
                );
            case FloatType.ID:
                return new FloatCollectSetNumericDocValueAggregator(
                    fieldTypes.get(0).name(),
                    innerTypeEstimator
                );
            case DoubleType.ID:
                return new DoubleCollectSetNumericDocValueAggregator(
                    fieldTypes.get(0).name(),
                    innerTypeEstimator
                );
            case IpType.ID:
            case StringType.ID:
                return new CollectSetBinaryDocValueAggregator(
                    fieldTypes.get(0).name(),
                    innerTypeEstimator
                );
            default:
                return null;
        }
    }

    private static class LongCollectSetNumericDocValueAggregator implements DocValueAggregator<HashMap<Object, Object>> {

        private final String columnName;
        private final DataType<?> dataType;
        private final SizeEstimator<Object> sizeEstimator;

        private SortedNumericDocValues values;

        public LongCollectSetNumericDocValueAggregator(String columnName,
                                                       DataType<?> dataType,
                                                       SizeEstimator<Object> sizeEstimator) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public HashMap<Object, Object> initialState(RamAccounting ramAccounting) {
            var state = new HashMap<>();
            ramAccounting.addBytes(RamUsageEstimator.sizeOfMap(state));
            return state;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, HashMap<Object, Object> state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var value = dataType.sanitizeValue(values.nextValue());
                ramAccounting.addBytes(
                    RamUsageEstimator.alignObjectSize(sizeEstimator.estimateSize(value) + 48L));
                state.put(value, PRESENT);
            }
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, HashMap<Object, Object> state) {
            return state;
        }
    }

    private static class FloatCollectSetNumericDocValueAggregator implements DocValueAggregator<HashMap<Float, Object>> {

        private final String columnName;
        private final SizeEstimator<Object> sizeEstimator;

        private SortedNumericDocValues values;


        public FloatCollectSetNumericDocValueAggregator(String columnName,
                                                        SizeEstimator<Object> sizeEstimator) {
            this.columnName = columnName;
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public HashMap<Float, Object> initialState(RamAccounting ramAccounting) {
            var state = new HashMap<Float, Object>();
            ramAccounting.addBytes(RamUsageEstimator.sizeOfMap(state));
            return state;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, HashMap<Float, Object> state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                ramAccounting.addBytes(
                    RamUsageEstimator.alignObjectSize(sizeEstimator.estimateSize(value) + 48L));
                state.put(value, PRESENT);
            }
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, HashMap<Float, Object> state) {
            return state;
        }
    }

    private static class DoubleCollectSetNumericDocValueAggregator implements DocValueAggregator<HashMap<Double, Object>> {

        private final String columnName;
        private final SizeEstimator<Object> sizeEstimator;

        private SortedNumericDocValues values;


        public DoubleCollectSetNumericDocValueAggregator(String columnName,
                                                         SizeEstimator<Object> sizeEstimator) {
            this.columnName = columnName;
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public HashMap<Double, Object> initialState(RamAccounting ramAccounting) {
            var state = new HashMap<Double, Object>();
            ramAccounting.addBytes(RamUsageEstimator.sizeOfMap(state));
            return state;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, HashMap<Double, Object> state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var value = NumericUtils.sortableLongToDouble(values.nextValue());
                ramAccounting.addBytes(
                    RamUsageEstimator.alignObjectSize(sizeEstimator.estimateSize(value) + 48L));
                state.put(value, PRESENT);
            }
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, HashMap<Double, Object> state) {
            return state;
        }
    }

    private static class CollectSetBinaryDocValueAggregator implements DocValueAggregator<HashMap<String, Object>> {

        private final String columnName;
        private final SizeEstimator<Object> sizeEstimator;

        private SortedBinaryDocValues values;

        public CollectSetBinaryDocValueAggregator(String columnName,
                                                  SizeEstimator<Object> sizeEstimator) {
            this.columnName = columnName;
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public HashMap<String, Object> initialState(RamAccounting ramAccounting) {
            var state = new HashMap<String, Object>();
            ramAccounting.addBytes(RamUsageEstimator.sizeOfMap(state));
            return state;
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = FieldData.toString(DocValues.getSortedSet(reader, columnName));
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, HashMap<String, Object> state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                var value = values.nextValue().utf8ToString();
                ramAccounting.addBytes(
                    RamUsageEstimator.alignObjectSize(sizeEstimator.estimateSize(value) + 48L));
                state.put(value, PRESENT);
            }
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, HashMap<String, Object> state) {
            return state;
        }
    }
}
