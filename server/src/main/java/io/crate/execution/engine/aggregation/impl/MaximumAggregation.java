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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;

import io.crate.common.MutableDouble;
import io.crate.common.MutableFloat;
import io.crate.common.MutableLong;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
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

public abstract class MaximumAggregation extends AggregationFunction<Object, Object> {

    public static final String NAME = "max";

    public static void register(Functions.Builder builder) {
        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            var fixedWidthType = supportedType instanceof FixedWidthType;
            builder.add(
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


    private static class LongMax implements DocValueAggregator<MutableLong> {

        private final String columnName;
        private final DataType<?> partialType;
        private SortedNumericDocValues values;

        public LongMax(String columnName, DataType<?> partialType) {
            this.columnName = columnName;
            this.partialType = partialType;
        }

        @Override
        public MutableLong initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(DataTypes.LONG.fixedSize());
            return new MutableLong(Long.MIN_VALUE);
        }

        @Override
        public void loadDocValues(LeafReaderContext reader) throws IOException {
            values = DocValues.getSortedNumeric(reader.reader(), columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableLong state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                long value = values.nextValue();
                if (value >= state.value()) {
                    state.setValue(value);
                }
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableLong state) {
            if (state.hasValue()) {
                return partialType.sanitizeValue(state.value());
            } else {
                return null;
            }
        }
    }


    private static class DoubleMax implements DocValueAggregator<MutableDouble> {

        private final String columnName;
        private SortedNumericDocValues values;

        public DoubleMax(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public MutableDouble initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(DataTypes.DOUBLE.fixedSize());
            return new MutableDouble(- Double.MAX_VALUE);
        }

        @Override
        public void loadDocValues(LeafReaderContext reader) throws IOException {
            values = DocValues.getSortedNumeric(reader.reader(), columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableDouble state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                double value = NumericUtils.sortableLongToDouble(values.nextValue());
                if (value >= state.value()) {
                    state.setValue(value);
                }
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableDouble state) {
            if (state.hasValue()) {
                return state.value();
            } else {
                return null;
            }
        }
    }


    private static class FloatMax implements DocValueAggregator<MutableFloat> {

        private final String columnName;
        private SortedNumericDocValues values;

        public FloatMax(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public MutableFloat initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            ramAccounting.addBytes(DataTypes.FLOAT.fixedSize());
            return new MutableFloat(- Float.MAX_VALUE);
        }

        @Override
        public void loadDocValues(LeafReaderContext reader) throws IOException {
            values = DocValues.getSortedNumeric(reader.reader(), columnName);
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableFloat state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                float value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                if (value >= state.value()) {
                    state.setValue(value);
                }
            }
        }

        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableFloat state) {
            if (state.hasValue()) {
                return state.value();
            } else {
                return null;
            }
        }
    }

    private static class FixedMaximumAggregation extends MaximumAggregation {

        private final int size;

        public FixedMaximumAggregation(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
            size = ((FixedWidthType) partialType()).fixedSize();
        }

        @Nullable
        @Override
        public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                           List<Reference> aggregationReferences,
                                                           DocTableInfo table,
                                                           List<Literal<?>> optionalParams) {
            Reference reference = aggregationReferences.get(0);
            if (!reference.hasDocValues()) {
                return null;
            }
            DataType<?> arg = reference.valueType();
            switch (arg.id()) {
                case ByteType.ID:
                case ShortType.ID:
                case IntegerType.ID:
                case LongType.ID:
                case TimestampType.ID_WITH_TZ:
                case TimestampType.ID_WITHOUT_TZ:
                    return new LongMax(reference.storageIdent(), arg);

                case FloatType.ID:
                    return new FloatMax(reference.storageIdent());
                case DoubleType.ID:
                    return new DoubleMax(reference.storageIdent());

                default:
                    return null;
            }
        }

        @Nullable
        @Override
        public Object newState(RamAccounting ramAccounting,
                               Version indexVersionCreated,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
            ramAccounting.addBytes(size);
            return null;
        }

        @Override
        public Object reduce(RamAccounting ramAccounting, Object state1, Object state2) {
            if (state1 == null) {
                return state2;
            }
            if (state2 == null) {
                return state1;
            }
            int cmp = type.compare(state1, state2);
            if (cmp < 0) {
                return state2;
            }
            return state1;
        }
    }

    private static class VariableMaximumAggregation extends MaximumAggregation {


        VariableMaximumAggregation(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
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
        public Object reduce(RamAccounting ramAccounting, Object state1, Object state2) {
            if (state1 == null) {
                if (state2 != null) {
                    ramAccounting.addBytes(type.valueBytes(state2));
                }
                return state2;
            }
            if (state2 == null) {
                return state1;
            }
            if (type.compare(state1, state2) < 0) {
                long delta = type.valueBytes(state1) - type.valueBytes(state2);
                ramAccounting.addBytes(delta);
                return state2;
            }
            return state1;
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    protected final DataType<Object> type;

    @SuppressWarnings("unchecked")
    private MaximumAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.type = (DataType<Object>) boundSignature.returnType();
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public DataType<?> partialType() {
        return boundSignature.returnType();
    }

    @Override
    public Object iterate(RamAccounting ramAccounting,
                          MemoryManager memoryManager,
                          Object state,
                          Input<?>... args) throws CircuitBreakingException {
        Object value = args[0].value();
        return reduce(ramAccounting, state, value);
    }

    @Override
    public Object terminatePartial(RamAccounting ramAccounting, Object state) {
        return state;
    }
}
