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
import java.math.BigInteger;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.BinaryDocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.execution.engine.aggregation.statistics.NumericVariance;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.NumericStorage;
import io.crate.types.NumericType;

public abstract class NumericStandardDeviationAggregation<V extends NumericVariance>
    extends AggregationFunction<V, BigDecimal> {

    public abstract static class StdDevNumericStateType<V extends NumericVariance>
        extends DataType<V> implements Streamer<V> {

        @Override
        public Precedence precedence() {
            return Precedence.CUSTOM;
        }

        @Override
        public Streamer<V> streamer() {
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V sanitizeValue(Object value) {
            return (V) value;
        }

        @Override
        public int compare(V val1, V val2) {
            return val1.compareTo(val2);
        }

        @Override
        public void writeValueTo(StreamOutput out, V v) throws IOException {
            v.writeTo(out);
        }

        @Override
        public long valueBytes(V value) {
            return value != null ? value.size() : 0L;
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public NumericStandardDeviationAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
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
    public V iterate(RamAccounting ramAccounting,
                     MemoryManager memoryManager,
                     V state,
                     Input<?>... args) throws CircuitBreakingException {
        if (state != null) {
            BigDecimal value = (BigDecimal) args[0].value();
            if (value != null) {
                long sizeBefore = state.size();
                state.increment(value);
                long sizeAfter = state.size();
                ramAccounting.addBytes(sizeBefore - sizeAfter);
            }
        }
        return state;
    }

    @Override
    public V reduce(RamAccounting ramAccounting, V state1, V state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.merge(state2);
        return state1;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public V removeFromAggregatedState(RamAccounting ramAccounting, V previousAggState, Input<?>[]stateToRemove) {
        if (previousAggState != null) {
            BigDecimal value = (BigDecimal) stateToRemove[0].value();
            if (value != null) {
                long sizeBefore = previousAggState.size();
                previousAggState.decrement(value);
                long sizeAfter = previousAggState.size();
                ramAccounting.addBytes(sizeBefore - sizeAfter);
            }
        }
        return previousAggState;
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, V state) {
        return state.result();
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       Version shardCreatedVersion,
                                                       List<Literal<?>> optionalParams) {
        Reference reference = aggregationReferences.get(0);
        if (reference == null) {
            return null;
        }
        if (!reference.hasDocValues()) {
            return null;
        }

        NumericType numericType = (NumericType) reference.valueType();
        Integer precision = numericType.numericPrecision();
        Integer scale = numericType.scale();
        if (precision == null || scale == null) {
            throw new UnsupportedOperationException(
                    "NUMERIC type requires precision and scale to support aggregation");
        }
        if (precision <= NumericStorage.COMPACT_PRECISION) {
            return new SortedNumericDocValueAggregator<>(
                    reference.storageIdent(),
                    (ramAccounting, memoryManager, version) ->
                        newState(ramAccounting, version, memoryManager),
                    (ramAccounting, values, state) -> {
                        long docValue = values.nextValue();
                        long sizeBefore = state.size();
                        state.increment(BigDecimal.valueOf(docValue, scale));
                        long sizeAfter = state.size();
                        ramAccounting.addBytes(sizeBefore - sizeAfter);
                    }
            );
        } else {
            return new BinaryDocValueAggregator<>(
                    reference.storageIdent(),
                    (ramAccounting, memoryManager, version) ->
                        newState(ramAccounting, version, memoryManager),
                    (ramAccounting, values, state) -> {
                        BytesRef bytesRef = values.lookupOrd(values.nextOrd());
                        BigInteger bigInteger = NumericUtils.sortableBytesToBigInt(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                        long sizeBefore = state.size();
                        state.increment(new BigDecimal(bigInteger, scale));
                        long sizeAfter = state.size();
                        ramAccounting.addBytes(sizeBefore - sizeAfter);
                    }
            );
        }
    }
}
