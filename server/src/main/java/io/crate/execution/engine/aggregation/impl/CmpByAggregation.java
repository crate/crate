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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;
import io.crate.types.TypeSignature;

public final class CmpByAggregation extends AggregationFunction<CmpByAggregation.CompareBy, Object> {

    public static final String MAX_BY = "max_by";
    public static final String MIN_BY = "min_by";

    static class CompareBy {

        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CompareBy.class);

        Comparable<Object> cmpValue;
        Object resultValue;

        @Override
        public int hashCode() {
            return Objects.hash(cmpValue, resultValue);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CompareBy other = (CompareBy) obj;
            return Objects.equals(cmpValue, other.cmpValue) && Objects.equals(resultValue, other.resultValue);
        }

        @Override
        public String toString() {
            return "CompareBy{cmpValue=" + cmpValue + ", resultValue=" + resultValue + "}";
        }
    }

    static {
        DataTypes.register(CompareByType.ID, CompareByType::new);
    }

    public static void register(Functions.Builder builder) {
        TypeSignature returnValueType = TypeSignature.parse("A");
        TypeSignature cmpType = TypeSignature.parse("B");
        var variableConstraintA = TypeVariableConstraint.typeVariableOfAnyType("A");
        var variableConstraintB = TypeVariableConstraint.typeVariableOfAnyType("B");
        builder.add(
            Signature.builder(MAX_BY, FunctionType.AGGREGATE)
                .argumentTypes(returnValueType,
                        cmpType)
                .returnType(returnValueType)
                .features(Scalar.Feature.DETERMINISTIC)
                .typeVariableConstraints(variableConstraintA, variableConstraintB)
                .build(),
            (signature, boundSignature) -> new CmpByAggregation(1, signature, boundSignature)
        );
        builder.add(
            Signature.builder(MIN_BY, FunctionType.AGGREGATE)
                .argumentTypes(returnValueType,
                        cmpType)
                .returnType(returnValueType)
                .features(Scalar.Feature.DETERMINISTIC)
                .typeVariableConstraints(variableConstraintA, variableConstraintB)
                .build(),
            (signature, boundSignature) -> new CmpByAggregation(-1, signature, boundSignature)
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final CompareByType partialType;
    private final int cmpResult;

    public CmpByAggregation(int cmpResult, Signature signature, BoundSignature boundSignature) {
        this.cmpResult = cmpResult;
        this.signature = signature;
        this.boundSignature = boundSignature;
        int size = boundSignature.argTypes().size();
        if (size == 1) {
            this.partialType = (CompareByType) boundSignature.argTypes().get(0);
        } else {
            this.partialType = new CompareByType(
                boundSignature.argTypes().get(0),
                boundSignature.argTypes().get(1)
            );
        }
    }

    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        Reference searchRef = aggregationReferences.get(1);
        if (!searchRef.hasDocValues()) {
            return null;
        }
        DataType<?> searchType = searchRef.valueType();
        switch (searchType.id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                var resultExpression = referenceResolver.getImplementation(aggregationReferences.get(0));
                if (signature.getName().name().equalsIgnoreCase("min_by")) {
                    return new MinByLong(
                        searchRef.storageIdent(),
                        searchType,
                        resultExpression,
                        new CollectorContext(table.droppedColumns(), table.lookupNameBySourceKey())
                    );
                } else {
                    return new MaxByLong(
                        searchRef.storageIdent(),
                        searchType,
                        resultExpression,
                        new CollectorContext(table.droppedColumns(), table.lookupNameBySourceKey())
                    );
                }
            default:
                return null;
        }
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
    @Nullable
    public CompareBy newState(RamAccounting ramAccounting,
                              Version indexVersionCreated,
                              Version minNodeInCluster,
                              MemoryManager memoryManager) {
        ramAccounting.addBytes(CompareBy.SHALLOW_SIZE);
        return new CompareBy();
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public CompareBy iterate(RamAccounting ramAccounting,
                             MemoryManager memoryManager,
                             CompareBy state,
                             Input<?>... args) throws CircuitBreakingException {
        Object cmpVal = args[1].value();

        if (cmpVal instanceof Comparable comparable) {
            Comparable<Object> currentValue = state.cmpValue;
            if (currentValue == null || comparable.compareTo(currentValue) == cmpResult) {
                state.cmpValue = comparable;
                state.resultValue = args[0].value();
            }
        } else if (cmpVal != null) {
            throw new UnsupportedOperationException(
                "Cannot use `" + signature.getName().displayName() + "` on values of type " + boundSignature.argTypes().get(1).getName());
        }
        return state;
    }

    @Override
    public CompareBy reduce(RamAccounting ramAccounting,
                            CompareBy state1,
                            CompareBy state2) {
        if (state1.cmpValue == null) {
            return state2;
        }
        if (state2.cmpValue == null) {
            return state1;
        }
        if (state2.cmpValue.compareTo(state1.cmpValue) == cmpResult) {
            return state2;
        } else {
            return state1;
        }
    }

    @Override
    public Object terminatePartial(RamAccounting ramAccounting, CompareBy state) {
        return state.resultValue;
    }

    @Override
    public DataType<?> partialType() {
        return partialType;
    }


    static class CmpByLongState {

        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CmpByLongState.class);

        long cmpValue;
        int docId;
        LeafReaderContext leafReaderContext;
        boolean hasValue = false;

        public CmpByLongState(long cmpValue) {
            this.cmpValue = cmpValue;
        }
    }

    static class MinByLong extends CmpByLong {

        public MinByLong(String columnName,
                         DataType<?> searchType,
                         LuceneCollectorExpression<?> resultExpression,
                         CollectorContext collectorContext) {
            super(Long.MAX_VALUE, columnName, searchType, resultExpression, collectorContext);
        }

        @Override
        boolean hasPrecedence(long currentValue, long stateValue) {
            return currentValue < stateValue;
        }
    }

    static class MaxByLong extends CmpByLong {

        public MaxByLong(String columnName,
                         DataType<?> searchType,
                         LuceneCollectorExpression<?> resultExpression,
                         CollectorContext collectorContext) {
            super(Long.MIN_VALUE, columnName, searchType, resultExpression,collectorContext);
        }

        @Override
        boolean hasPrecedence(long currentValue, long stateValue) {
            return currentValue > stateValue;
        }
    }

    abstract static class CmpByLong implements DocValueAggregator<CmpByLongState> {

        private final String columnName;
        private final LuceneCollectorExpression<?> resultExpression;
        private final DataType<?> searchType;
        private final long sentinelValue;

        private SortedNumericDocValues values;
        private LeafReaderContext leafReaderContext;

        CmpByLong(long sentinelValue,
                  String columnName,
                  DataType<?> searchType,
                  LuceneCollectorExpression<?> resultExpression,
                  CollectorContext collectorContext) {
            this.sentinelValue = sentinelValue;
            this.columnName = columnName;
            this.searchType = searchType;
            this.resultExpression = resultExpression;
            resultExpression.startCollect(collectorContext);
        }

        @Override
        public CmpByLongState initialState(RamAccounting ramAccounting,
                                           MemoryManager memoryManager,
                                           Version minNodeVersion) {
            ramAccounting.addBytes(CmpByLongState.SHALLOW_SIZE);
            return new CmpByLongState(sentinelValue);
        }

        @Override
        public void loadDocValues(LeafReaderContext leafReaderContext) throws IOException {
            this.leafReaderContext = leafReaderContext;
            values = DocValues.getSortedNumeric(leafReaderContext.reader(), columnName);
        }

        abstract boolean hasPrecedence(long currentValue, long stateValue);

        @Override
        public void apply(RamAccounting ramAccounting, int doc, CmpByLongState state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                long value = values.nextValue();
                if (hasPrecedence(value, state.cmpValue)) {
                    state.hasValue = true;
                    state.cmpValue = value;

                    state.docId = doc;
                    state.leafReaderContext = this.leafReaderContext;
                }
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Object partialResult(RamAccounting ramAccounting, CmpByLongState state) {
            CompareBy compareBy = new CompareBy();
            if (state.hasValue) {
                compareBy.cmpValue = (Comparable) searchType.sanitizeValue(state.cmpValue);
                try {
                    resultExpression.setNextReader(new ReaderContext(state.leafReaderContext));
                    resultExpression.setNextDocId(state.docId);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
                compareBy.resultValue = resultExpression.value();
            }
            return compareBy;
        }
    }
}
