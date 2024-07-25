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
import org.apache.datasketches.frequencies.LongsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

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
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
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
                state = initState(ramAccounting, limit);

            } else if (args.length == 1) {
                state = initState(ramAccounting, DEFAULT_LIMIT);
            }
        }
        state.update(value);
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
        return state1.merge(state2);
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

        if (optionalParams.isEmpty()) {
            return getDocValueAggregator(reference, DEFAULT_LIMIT);
        }

        Literal<?> limit = optionalParams.getLast();
        if (limit == null) {
            return getDocValueAggregator(reference, DEFAULT_LIMIT);
        }
        return getDocValueAggregator(reference, (int) limit.value());
    }

    @Nullable
    private DocValueAggregator<?> getDocValueAggregator(Reference ref, int limit) {
        DataType<?> type = ref.valueType();
        if (supportedByLongSketch(type)) {
            return new SortedNumericDocValueAggregator<>(
                ref.storageIdent(),
                (ramAccounting, _, _) -> topKLongState(ramAccounting, type, limit),
                (values, state) -> {
                    state.update(values.nextValue());
                });
        } else if (type.id() == StringType.ID) {
            return new BinaryDocValueAggregator<>(
                ref.storageIdent(),
                (ramAccounting, _, _) -> topKState(ramAccounting, limit),
                (values, state) -> {
                    long ord = values.nextOrd();
                    BytesRef value = values.lookupOrd(ord);
                    state.update(value.utf8ToString());
                });
        }
        return null;
    }

    abstract static sealed class State {

        static final Empty EMPTY = new Empty();

        abstract List<Map<String, Object>> result();

        abstract int limit();

        abstract int id();

        abstract State merge(State other);

        abstract void update(Object value);

        abstract void update(long value);

        void writeTo(StreamOutput out, DataType<?> innerType) throws IOException {
            out.writeByte((byte) id());
        }

        static State fromStream(StreamInput in, DataType<?> innerType) throws IOException {
            int id = in.readByte();
            switch (id) {
                case TopKState.ID -> {
                    return new TopKState(in, innerType);
                }
                case TopKLongState.ID -> {
                    return new TopKLongState(in, innerType);
                }
                default -> {
                    return State.EMPTY;
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return id() == state.id() && limit() == state.limit() && Objects.equals(result(), state.result());
        }

        @Override
        public int hashCode() {
            return Objects.hash(id(), result(), limit());
        }

    }

    static final class Empty extends State {

        static final int ID = 0;

        @Override
        List<Map<String, Object>> result() {
            return List.of();
        }

        @Override
        int limit() {
            return -1;
        }

        @Override
        int id() {
            return ID;
        }

        @Override
        State merge(State other) {
            return other;
        }

        @Override
        void update(Object value) {
            throw new UnsupportedOperationException("Empty state does not support updates");
        }

        @Override
        void update(long value) {
            throw new UnsupportedOperationException("Empty state does not support updates");
        }
    }

    static final class TopKState extends State {

        static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopKState.class);
        static final int ID = 1;

        private final ItemsSketch<Object> sketch;
        private final int limit;

        TopKState(ItemsSketch<Object> sketch, int limit) {
            this.sketch = sketch;
            this.limit = limit;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        TopKState(StreamInput in, DataType<?> innerType) throws IOException {
            this.limit = in.readInt();
            SketchStreamer streamer = new SketchStreamer(innerType.streamer());
            this.sketch = ItemsSketch.getInstance(Memory.wrap(in.readByteArray()), streamer);
        }

        public List<Map<String, Object>> result() {
            if (sketch.isEmpty()) {
                return List.of();
            }
            ItemsSketch.Row<Object>[] frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
            int limit = Math.min(frequentItems.length, this.limit);
            var result = new ArrayList<Map<String, Object>>(limit);
            for (int i = 0; i < limit; i++) {
                var item = frequentItems[i];
                result.add(Map.of("item", item.getItem(), "frequency", item.getEstimate()));
            }
            return result;
        }

        @Override
        int limit() {
            return limit;
        }

        @Override
        int id() {
            return ID;
        }

        @Override
        State merge(State other) {
            if (other instanceof Empty) {
                return this;
            } else if (other instanceof TopKState otherTopk) {
                return new TopKState(this.sketch.merge(otherTopk.sketch), limit);
            }
            throw new IllegalArgumentException("Cannot merge state");
        }

        @Override
        void update(Object value) {
            sketch.update(value);
        }

        @Override
        void update(long value) {
            sketch.update(value);
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        void writeTo(StreamOutput out, DataType<?> innerType) throws IOException {
            super.writeTo(out, innerType);
            out.writeInt(limit);
            SketchStreamer streamer = new SketchStreamer(innerType.streamer());
            out.writeByteArray(sketch.toByteArray(streamer));
        }

    }

    static final class TopKLongState extends State {

        static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopKLongState.class);
        static final int ID = 2;

        private final LongsSketch sketch;
        private final DataType<?> dataType;
        private final int limit;

        TopKLongState(LongsSketch sketch, DataType<?> dataType, int limit) {
            this.sketch = sketch;
            this.dataType = dataType;
            this.limit = limit;
        }

        TopKLongState(StreamInput in, DataType<?> dataType) throws IOException {
            this.limit = in.readInt();
            this.sketch = LongsSketch.getInstance(Memory.wrap(in.readByteArray()));
            this.dataType = dataType;
        }

        public List<Map<String, Object>> result() {
            if (sketch.isEmpty()) {
                return List.of();
            }
            LongsSketch.Row[] frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
            int limit = Math.min(frequentItems.length, this.limit);
            var result = new ArrayList<Map<String, Object>>(limit);
            for (int i = 0; i < limit; i++) {
                var item = frequentItems[i];
                result.add(Map.of("item", toObject(dataType, item.getItem()), "frequency", item.getEstimate()));
            }
            return result;
        }

        @Override
        State merge(State other) {
            if (other instanceof Empty) {
                return this;
            } else if (other instanceof TopKLongState otherTopK) {
                return new TopKLongState(this.sketch.merge(otherTopK.sketch), dataType, limit);
            }
            throw new IllegalArgumentException("Cannot merge state");
        }

        @Override
        void update(Object value) {
            sketch.update(toLong(dataType, value));
        }

        @Override
        void update(long value) {
            sketch.update(value);
        }

        @Override
        void writeTo(StreamOutput out, DataType<?> innerType) throws IOException {
            super.writeTo(out, innerType);
            out.writeInt(limit);
            out.writeByteArray(sketch.toByteArray());
        }

        @Override
        int limit() {
            return limit;
        }

        @Override
        int id() {
            return ID;
        }
    }

    private static boolean supportedByLongSketch(DataType<?> type) {
        return switch (type.id()) {
            case LongType.ID -> true;
            case DoubleType.ID -> true;
            case FloatType.ID -> true;
            default -> false;
        };
    }

    private static long toLong(DataType<?> type, Object o) {
        return switch (type.id()) {
            case LongType.ID -> (Long) o;
            case DoubleType.ID -> NumericUtils.doubleToSortableLong((Double) o);
            case FloatType.ID -> (long) NumericUtils.floatToSortableInt((Float) o);
            default -> throw new IllegalArgumentException("Type cannot be converted to long");
        };
    }

    private static Object toObject(DataType<?> type, long o) {
        return switch (type.id()) {
            case LongType.ID -> o;
            case DoubleType.ID -> NumericUtils.sortableLongToDouble(o);
            case FloatType.ID -> NumericUtils.sortableIntToFloat((int) o);
            default -> throw new IllegalArgumentException("Long value cannot be converted");
        };
    }

    private State initState(RamAccounting ramAccounting, int limit) {
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException(
                "Limit parameter for topk must be between 0 and 10_000. Got: " + limit);
        }
        DataType<?> dataType = boundSignature.argTypes().getFirst();
        if (supportedByLongSketch(dataType)) {
            return topKLongState(ramAccounting, dataType, limit);
        } else {
            return topKState(ramAccounting, limit);
        }
    }

    private TopKState topKState(RamAccounting ramAccounting, int limit) {
        int maxMapSize = maxMapSize(limit);
        ramAccounting.addBytes(calculateRamUsage(maxMapSize) + TopKState.SHALLOW_SIZE);
        return new TopKState(new ItemsSketch<>(maxMapSize), limit);
    }

    private TopKLongState topKLongState(RamAccounting ramAccounting, DataType<?> dataType, int limit) {
        int maxMapSize = maxMapSize(limit);
        ramAccounting.addBytes(calculateRamUsage(maxMapSize) + TopKLongState.SHALLOW_SIZE);
        return new TopKLongState(new LongsSketch(maxMapSize), dataType, limit);
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
        public State readValueFrom(StreamInput in) throws IOException {
            return State.fromStream(in, innerType);
        }

        @Override
        public void writeValueTo(StreamOutput out, State state) throws IOException {
            state.writeTo(out, innerType);
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
