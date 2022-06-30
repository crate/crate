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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.common.MutableObject;
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
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

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
    public DocValueAggregator<?> getDocValueAggregator(List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        Reference arg = aggregationReferences.get(0);
        if (!arg.hasDocValues()) {
            return null;
        }
        var dataType = arg.valueType();
        switch (dataType.id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                return new LongArbitraryDocValueAggregator(
                    arg.column().fqn(),
                    dataType,
                    partialEstimator
                );
            case FloatType.ID:
                return new FloatArbitraryDocValueAggregator(
                    arg.column().fqn(),
                    partialEstimator
                );
            case DoubleType.ID:
                return new DoubleArbitraryDocValueAggregator(
                    arg.column().fqn(),
                    partialEstimator
                );
            case IpType.ID:
            case StringType.ID:
                return new ArbitraryBinaryDocValueAggregator(
                    arg.column().fqn(),
                    dataType,
                    partialEstimator
                );
            default:
                return null;
        }
    }

    private static class LongArbitraryDocValueAggregator extends ArbitraryNumericDocValueAggregator {

        private final DataType<?> dataType;
        private final SizeEstimator<Object> sizeEstimator;

        public LongArbitraryDocValueAggregator(String columnName,
                                               DataType<?> dataType,
                                               SizeEstimator<Object> sizeEstimator) {
            super(columnName);
            this.dataType = dataType;
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableObject state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (!state.hasValue()) {
                    var value = dataType.sanitizeValue(values.nextValue());
                    ramAccounting.addBytes(sizeEstimator.estimateSize(value));
                    state.setValue(value);
                }
            }
        }
    }

    private static class FloatArbitraryDocValueAggregator extends ArbitraryNumericDocValueAggregator {

        private final SizeEstimator<Object> sizeEstimator;

        public FloatArbitraryDocValueAggregator(String columnName,
                                                SizeEstimator<Object> sizeEstimator) {
            super(columnName);
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableObject state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (!state.hasValue()) {
                    var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                    ramAccounting.addBytes(sizeEstimator.estimateSize(value));
                    state.setValue(value);
                }
            }
        }
    }

    private static class DoubleArbitraryDocValueAggregator extends ArbitraryNumericDocValueAggregator {

        private final SizeEstimator<Object> sizeEstimator;

        public DoubleArbitraryDocValueAggregator(String columnName,
                                                 SizeEstimator<Object> sizeEstimator) {
            super(columnName);
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableObject state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (!state.hasValue()) {
                    var value = NumericUtils.sortableLongToDouble(values.nextValue());
                    ramAccounting.addBytes(sizeEstimator.estimateSize(value));
                    state.setValue(value);
                }
            }
        }
    }

    private abstract static class ArbitraryNumericDocValueAggregator implements DocValueAggregator<MutableObject> {

        private final String columnName;

        protected SortedNumericDocValues values;

        public ArbitraryNumericDocValueAggregator(String columnName) {
            this.columnName = columnName;
        }

        @Override
        public MutableObject initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            return new MutableObject();
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = DocValues.getSortedNumeric(reader, columnName);
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableObject state) {
            if (state.hasValue()) {
                return state.value();
            } else {
                return null;
            }
        }
    }

    private static class ArbitraryBinaryDocValueAggregator implements DocValueAggregator<MutableObject> {

        private final String columnName;
        private final DataType<?> dataType;
        private final SizeEstimator<Object> sizeEstimator;

        private SortedBinaryDocValues values;

        public ArbitraryBinaryDocValueAggregator(String columnName,
                                                 DataType<?> dataType,
                                                 SizeEstimator<Object> sizeEstimator) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.sizeEstimator = sizeEstimator;
        }

        @Override
        public MutableObject initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            return new MutableObject();
        }

        @Override
        public void loadDocValues(LeafReader reader) throws IOException {
            values = FieldData.toString(DocValues.getSortedSet(reader, columnName));
        }

        @Override
        public void apply(RamAccounting ramAccounting, int doc, MutableObject state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                if (!state.hasValue()) {
                    var value = dataType.sanitizeValue(values.nextValue().utf8ToString());
                    ramAccounting.addBytes(sizeEstimator.estimateSize(value));
                    state.setValue(value);
                }
            }
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, MutableObject state) {
            if (state.hasValue()) {
                return state.value();
            } else {
                return null;
            }
        }
    }
}
