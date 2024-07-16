/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.RamUsageEstimator;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.BinaryDocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
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
import io.crate.statistics.SketchStreamer;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import io.crate.types.StringType;
import io.crate.types.TypeSignature;

public class TopKAggregation extends AggregationFunction<TopKAggregation.State, List<Map<String, Object>>> {

    public static final String NAME = "topk";

    static final Signature DEFAULT_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.parse("V"))
            .returnType(new ArrayType<>(DataTypes.UNTYPED_OBJECT).getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .typeVariableConstraints(typeVariable("V"))
            .build();

    static final Signature PARAMETER_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.parse("V"),
                DataTypes.INTEGER.getTypeSignature())
            .returnType(new ArrayType<>(DataTypes.UNTYPED_OBJECT).getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .typeVariableConstraints(typeVariable("V"))
            .build();

    static {
        DataTypes.register(StateType.ID, StateType::new);
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            DEFAULT_SIGNATURE,
            TopKAggregation::new
        );

        builder.add(
            PARAMETER_SIGNATURE,
            TopKAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    private static final int DEFAULT_LIMIT = 8;
    private static final int MAX_LIMIT = 10_000;

    private TopKAggregation(Signature signature, BoundSignature boundSignature) {
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


    @Nullable
    @Override
    public State newState(RamAccounting ramAccounting,
                          Version indexVersionCreated,
                          Version minNodeInCluster,
                          MemoryManager memoryManager) {
        return State.EMPTY;
    }

    @Override
    public State iterate(RamAccounting ramAccounting,
                         MemoryManager memoryManager,
                         State state,
                         Input<?>... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (state instanceof Empty) {
            if (args.length == 2) {
                // We have a limit provided by the user
                Integer limit = (Integer) args[1].value();
                if (limit <= 0 || limit > MAX_LIMIT) {
                    throw new IllegalArgumentException(
                        "Limit parameter for topk must be between 0 and 10_000. Got: " + limit);
                }
                state = topKState(ramAccounting, limit);

            } else if (args.length == 1) {
                state = topKState(ramAccounting, DEFAULT_LIMIT);
            }
        }
        if (state instanceof TopKState topKState) {
            topKState.itemsSketch.update(value);
        }
        return state;
    }

    private static int maxMapSize(int x) {
        // max map size should be 4 * the limit based on the power of 2 to avoid errors
        return (int) Math.pow(2, Math.ceil(Math.log(x) / Math.log(2))) * 4;
    }

    private static long calculateRamUsage(long maxMapSize) {
        // The internal memory space usage of item sketch will never exceed 18 * maxMapSize bytes, plus a small
        // constant number of additional bytes.
        // https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html
        return maxMapSize * 18L;
    }

    @Override
    public State reduce(RamAccounting ramAccounting, State state1, State state2) {
        if (state1 instanceof TopKState t1 && state2 instanceof TopKState t2) {
            return new TopKState(t1.itemsSketch.merge(t2.itemsSketch), t1.limit);
        } else if (state1 instanceof Empty) {
            return state2;
        } else {
            return state1;
        }
    }

    @Override
    public List<Map<String, Object>> terminatePartial(RamAccounting ramAccounting, State state) {
        return state.result();
    }

    public DataType<?> partialType() {
        return new StateType(boundSignature.argTypes().getFirst());
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        if (aggregationReferences.isEmpty()) {
            return null;
        }

        Reference reference = aggregationReferences.getFirst();
        if (reference == null) {
            return null;
        }

        if (!reference.hasDocValues()) {
            return null;
        }

        Literal<?> limit = optionalParams.getLast();
        if (limit == null) {
            return getDocValueAggregator(reference, DEFAULT_LIMIT);
        }

        return getDocValueAggregator(reference, (int) limit.value());
    }

    @Nullable
    private DocValueAggregator<?> getDocValueAggregator(Reference ref, int limit) {
        return switch (ref.valueType().id()) {
            case LongType.ID -> new SortedNumericDocValueAggregator<>(
                ref.storageIdent(),
                (ramAccounting, _, _) -> topKState(ramAccounting, limit),
                (values, state) -> {
                    state.itemsSketch.update(values.nextValue());
                }
            );
            case StringType.ID -> new BinaryDocValueAggregator<>(
                ref.storageIdent(),
                (ramAccounting, _, _) -> topKState(ramAccounting, limit),
                (values, state) -> {
                    long ord = values.nextOrd();
                    BytesRef value = values.lookupOrd(ord);
                    state.itemsSketch.update(value.utf8ToString());
                }
            );
            default -> null;
        };
    }

    sealed interface State {

        State EMPTY = new Empty();

        List<Map<String, Object>> result();
    }

    record Empty() implements State {

        @Override
        public List<Map<String, Object>> result() {
            return List.of();
        }
    }

    record TopKState(ItemsSketch<Object> itemsSketch, int limit) implements State {

        static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopKState.class);

        public List<Map<String, Object>> result() {
            if (itemsSketch.isEmpty()) {
                return List.of();
            }
            ItemsSketch.Row<Object>[] frequentItems = itemsSketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
            int limit = Math.min(frequentItems.length, this.limit);
            var result = new ArrayList<Map<String, Object>>(limit);
            for (int i = 0; i < limit; i++) {
                var item = frequentItems[i];
                result.add(Map.of("item", item.getItem(), "frequency", item.getEstimate()));
            }
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopKState topKState = (TopKState) o;
            return limit == topKState.limit && Objects.equals(result(), topKState.result());
        }

        @Override
        public int hashCode() {
            return Objects.hash(result(), limit);
        }
    }

    private static TopKState topKState(RamAccounting ramAccounting, int limit) {
        int maxMapSize = maxMapSize(limit);
        ramAccounting.addBytes(calculateRamUsage(maxMapSize));
        ramAccounting.addBytes(TopKState.SHALLOW_SIZE);
        return new TopKState(new ItemsSketch<>(maxMapSize), limit);
    }

    static final class StateType extends DataType<State> implements Streamer<State> {

        public static final int ID = 4232;
        private final DataType<?> innerType;

        public StateType(DataType<?> innerType) {
            this.innerType = innerType;
        }

        public StateType(StreamInput streamInput) throws IOException {
            this.innerType = DataTypes.fromStream(streamInput);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            DataTypes.toStream(innerType, out);
        }

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
            return "topk_state";
        }

        @Override
        public Streamer<State> streamer() {
            return this;
        }

        @Override
        public State sanitizeValue(Object value) {
            return (State) value;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public State readValueFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                return State.EMPTY;
            } else {
                int limit = in.readInt();
                SketchStreamer streamer = new SketchStreamer(innerType.streamer());
                return new TopKState(ItemsSketch.getInstance(Memory.wrap(in.readByteArray()), streamer), limit);
            }
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public void writeValueTo(StreamOutput out, State state) throws IOException {
            if (state instanceof Empty) {
                out.writeBoolean(true);
            } else if (state instanceof TopKState topkState) {
                out.writeBoolean(false);
                out.writeInt(topkState.limit);
                SketchStreamer streamer = new SketchStreamer(innerType.streamer());
                out.writeByteArray(topkState.itemsSketch.toByteArray(streamer));
            }
        }

        @Override
        public long valueBytes(State value) {
            throw new UnsupportedOperationException("valueSize is not implemented for TopKStateType");
        }

        @Override
        public int compare(State s1, State s2) {
            return 0;
        }
    }

}
