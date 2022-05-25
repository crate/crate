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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.common.MutableLong;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.BinaryDocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.MemoryManager;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.BitStringType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FixedWidthType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class CountAggregation extends AggregationFunction<MutableLong, Long> {

    public static final String NAME = "count";
    public static final Signature SIGNATURE =
        Signature.aggregate(
            NAME,
            parseTypeSignature("V"),
            DataTypes.LONG.getTypeSignature()
        ).withTypeVariableConstraints(typeVariable("V"));

    public static final Signature COUNT_STAR_SIGNATURE =
        Signature.aggregate(NAME, DataTypes.LONG.getTypeSignature());

    static {
        DataTypes.register(CountAggregation.LongStateType.ID, in -> CountAggregation.LongStateType.INSTANCE);
    }

    public static void register(AggregationImplModule mod) {
        mod.register(
            SIGNATURE,
            (signature, boundSignature) ->
                new CountAggregation(signature, boundSignature, true)
        );
        mod.register(
            COUNT_STAR_SIGNATURE,
            (signature, boundSignature) ->
                new CountAggregation(signature, boundSignature, false)
        );
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final boolean hasArgs;

    private CountAggregation(Signature signature, Signature boundSignature, boolean hasArgs) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.hasArgs = hasArgs;
    }

    @Override
    public MutableLong iterate(RamAccounting ramAccounting,
                               MemoryManager memoryManager,
                               MutableLong state,
                               Input... args) {
        if (!hasArgs || args[0].value() != null) {
            return state.add(1L);
        }
        return state;
    }

    @Nullable
    @Override
    public MutableLong newState(RamAccounting ramAccounting,
                                Version indexVersionCreated,
                                Version minNodeInCluster,
                                MemoryManager memoryManager) {
        ramAccounting.addBytes(LongStateType.INSTANCE.fixedSize());
        return new MutableLong(0L);
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
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert function.arguments().size() <= 1 : "function's number of arguments must be 0 or 1";

        if (function.arguments().size() == 1) {
            Symbol arg = function.arguments().get(0);
            if (arg instanceof Input) {
                if (((Input) arg).value() == null) {
                    return Literal.of(0L);
                } else {
                    return new Function(COUNT_STAR_SIGNATURE, List.of(), DataTypes.LONG);
                }
            }
        }
        return function;
    }

    @Override
    public DataType<?> partialType() {
        return LongStateType.INSTANCE;
    }

    @Override
    public MutableLong reduce(RamAccounting ramAccounting, MutableLong state1, MutableLong state2) {
        return state1.add(state2.value());
    }

    @Override
    public Long terminatePartial(RamAccounting ramAccounting, MutableLong state) {
        return state.value();
    }

    public static class LongStateType extends DataType<MutableLong>
        implements FixedWidthType, Streamer<MutableLong> {

        public static final int ID = 16384;
        public static final LongStateType INSTANCE = new LongStateType();

        @Override
        public int id() {
            return ID;
        }

        @Override
        public Precedence precedence() {
            return Precedence.CUSTOM;
        }

        @Override
        public String getName() {
            return "long_state";
        }

        @Override
        public Streamer<MutableLong> streamer() {
            return this;
        }

        @Override
        public MutableLong sanitizeValue(Object value) {
            return (MutableLong) value;
        }

        @Override
        public int compare(MutableLong val1, MutableLong val2) {
            if (val1 == null) {
                return -1;
            } else if (val2 == null) {
                return 1;
            } else {
                return Long.compare(val1.value(), val2.value());
            }
        }

        @Override
        public MutableLong readValueFrom(StreamInput in) throws IOException {
            return new MutableLong(in.readVLong());
        }

        @Override
        public void writeValueTo(StreamOutput out, MutableLong v) throws IOException {
            out.writeVLong(v.value());
        }

        @Override
        public int fixedSize() {
            return DataTypes.LONG.fixedSize();
        }
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public MutableLong removeFromAggregatedState(RamAccounting ramAccounting,
                                                 MutableLong previousAggState,
                                                 Input[] stateToRemove) {
        if (!hasArgs || stateToRemove[0].value() != null) {
            return previousAggState.sub(1L);
        }
        return previousAggState;
    }

    private DocValueAggregator<?> getDocValueAggregator(Reference ref) {
        if (!ref.hasDocValues()) {
            return null;
        }
        switch (ref.valueType().id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
            case FloatType.ID:
            case DoubleType.ID:
            case GeoPointType.ID:
                return new SortedNumericDocValueAggregator<>(
                    ref.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(LongStateType.INSTANCE.fixedSize());
                        return new MutableLong(0L);
                    },
                    (values, state) -> state.add(1L)
                );
            case IpType.ID:
            case StringType.ID:
            case BitStringType.ID:
                return new BinaryDocValueAggregator<>(
                    ref.column().fqn(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        ramAccounting.addBytes(LongStateType.INSTANCE.fixedSize());
                        return new MutableLong(0L);
                    },
                    (values, state) -> state.add(1L)
                );
            default:
                return null;
        }
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        if (aggregationReferences.size() != 1) {
            return null;
        }
        Reference reference = aggregationReferences.get(0);
        if (reference.valueType().id() == ObjectType.ID) {
            // Count on object would require loading the source just to check if there is a value.
            // Try to count on a non-null sub-column to be able to utilize doc-values.
            var aggregationRef = (Reference) aggregationReferences.get(0);
            for (var notNullCol : table.notNullColumns()) {
                // the first seen not-null sub-column will be used
                if (notNullCol.isChildOf(aggregationRef.column())) {
                    var notNullColRef = table.getReference(notNullCol);
                    if (notNullColRef == null) {
                        continue;
                    }
                    var subColDocValAggregator = getDocValueAggregator(notNullColRef);
                    if (subColDocValAggregator != null) {
                        return subColDocValAggregator;
                    }
                }
            }
        }
        if (!reference.hasDocValues()) {
            return null;
        }
        return getDocValueAggregator(reference);
    }
}
