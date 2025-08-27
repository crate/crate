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
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.execution.engine.aggregation.statistics.Variance;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FixedWidthType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;

public abstract class StandardDeviationAggregation<V extends Variance> extends AggregationFunction<V, Double> {

    public abstract static class StdDevStateType<V extends Variance>
        extends DataType<V> implements Streamer<V>, FixedWidthType {

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
        public int fixedSize() {
            return Variance.FIXED_SIZE;
        }

        @Override
        public void writeValueTo(StreamOutput out, V v) throws IOException {
            v.writeTo(out);
        }

        @Override
        public long valueBytes(V value) {
            return fixedSize();
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public StandardDeviationAggregation(Signature signature, BoundSignature boundSignature) {
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
            Number value = (Number) args[0].value();
            if (value != null) {
                state.increment(value.doubleValue());
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
            Number value = (Number) stateToRemove[0].value();
            if (value != null) {
                previousAggState.decrement(value.doubleValue());
            }
        }
        return previousAggState;
    }

    @Override
    public Double terminatePartial(RamAccounting ramAccounting, V state) {
        double result = state.result();
        return Double.isNaN(result) ? null : result;
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       Version shardCreatedVersion,
                                                       List<Literal<?>> optionalParams) {
        Reference reference = getAggReference(aggregationReferences);
        if (reference == null) {
            return null;
        }
        return switch (reference.valueType().id()) {
            case ByteType.ID, ShortType.ID, IntegerType.ID, LongType.ID, TimestampType.ID_WITH_TZ,
                 TimestampType.ID_WITHOUT_TZ -> new SortedNumericDocValueAggregator<>(
                     reference.storageIdent(),
                     (ramAccounting, memoryManager, version) -> {
                         ramAccounting.addBytes(V.fixedSize());
                         return newState(ramAccounting, version, memoryManager);
                     },
                     (_, values, state) -> state.increment(values.nextValue())
            );
            case FloatType.ID -> new SortedNumericDocValueAggregator<>(
                reference.storageIdent(),
                (ramAccounting, memoryManager, version) -> {
                    ramAccounting.addBytes(V.fixedSize());
                    return newState(ramAccounting, version, memoryManager);
                },
                (_, values, state) -> {
                    var value = NumericUtils.sortableIntToFloat((int) values.nextValue());
                    state.increment(value);
                }
            );
            case DoubleType.ID -> new SortedNumericDocValueAggregator<>(
                reference.storageIdent(),
                (ramAccounting, memoryManager, version) -> {
                    ramAccounting.addBytes(V.fixedSize());
                    return newState(ramAccounting, version, memoryManager);
                },
                (_, values, state) -> {
                    var value = NumericUtils.sortableLongToDouble((values.nextValue()));
                    state.increment(value);
                }
            );
            default -> null;
        };
    }
}
