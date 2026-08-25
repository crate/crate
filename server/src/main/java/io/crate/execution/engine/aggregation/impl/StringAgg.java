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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * string_agg :: text -> text -> text
 * string_agg(expression, delimiter)
 */
public final class StringAgg extends AggregationFunction<StringAgg.StringAggState, String> {

    static final String NAME = "string_agg";
    public static final Signature SIGNATURE =
            Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(DataTypes.STRING.getTypeSignature(),
                            DataTypes.STRING.getTypeSignature())
                    .returnType(DataTypes.STRING.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build();


    private static final int LIST_ENTRY_OVERHEAD = 32;

    static {
        DataTypes.register(StringAggStateType.INSTANCE.id(), _ -> StringAggStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            SIGNATURE,
            StringAgg::new
        );
    }

    public record StringAggState(List<String> values, List<String> delimiters) implements Writeable {

        private static final long SHALLOW_SIZE = 2 * ArrayType.ARRAY_LIST_SHALLOW_SIZE;

        StringAggState() {
            this(new ArrayList<>(), new ArrayList<>());
        }

        private StringAggState(StreamInput in) throws IOException {
            List<String> values;
            List<String> delimiters;
            if (in.getVersion().onOrAfter(Version.V_6_4_4)) {
                values = in.readStringList();
                int size = in.readVInt();
                delimiters = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    delimiters.add(in.readOptionalString());
                }
            } else {
                List<String> list = in.readStringList();
                values = new ArrayList<>(list.size() / 2);
                delimiters = new ArrayList<>(list.size() / 2);

                String firstDelimiter = in.readOptionalString();
                delimiters.add(firstDelimiter);

                Iterator<String> iterator = list.iterator();
                while (iterator.hasNext()) {
                    values.add(iterator.next());
                    delimiters.add(iterator.next());
                }
            }
            this(values, delimiters);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_6_4_4)) {
                out.writeStringCollection(values);
                out.writeVInt(delimiters.size());
                for (String delimiter : delimiters) {
                    out.writeOptionalString(delimiter);
                }
            } else {
                out.writeVInt(values.size() + delimiters.size());
                for (int i = 0; i < values.size(); i++) {
                    out.writeString(values.get(i));
                    if (i < values().size() - 1) {
                        out.writeString(delimiters.get(i + 1));
                    }
                }
                out.writeOptionalString(delimiters.isEmpty() ? null : delimiters.getFirst());
            }
        }
    }

    static class StringAggStateType extends DataType<StringAggState> implements Streamer<StringAggState> {

        static final StringAggStateType INSTANCE = new StringAggStateType();

        @Override
        public int id() {
            return 1025;
        }

        @Override
        public Precedence precedence() {
            return Precedence.CUSTOM;
        }

        @Override
        public String getName() {
            return "string_lists";
        }

        @Override
        public Streamer<StringAggState> streamer() {
            return this;
        }

        @Override
        public StringAggState sanitizeValue(Object value) {
            return (StringAggState) value;
        }

        @Override
        public int compare(StringAggState val1, StringAggState val2) {
            return 0;
        }

        @Override
        public StringAggState readValueFrom(StreamInput in) throws IOException {
            return new StringAggState(in);
        }

        @Override
        public void writeValueTo(StreamOutput out, StringAggState val) throws IOException {
            val.writeTo(out);
        }

        @Override
        public long valueBytes(StringAggState value) {
            throw new UnsupportedOperationException("valueSize is not implemented for StringAggStateType");
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public StringAgg(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public StringAggState newState(RamAccounting ramAccounting,
                                   Version minNodeInCluster,
                                   MemoryManager memoryManager) {
        ramAccounting.addBytes(StringAggState.SHALLOW_SIZE);
        return new StringAggState();
    }

    @Override
    public StringAggState iterate(RamAccounting ramAccounting,
                                  MemoryManager memoryManager,
                                  StringAggState state,
                                  Input<?>... args) throws CircuitBreakingException {
        String expression = (String) args[0].value();
        if (expression == null) {
            return state;
        }
        ramAccounting.addBytes(LIST_ENTRY_OVERHEAD + RamUsageEstimator.sizeOf(expression));
        String delimiter = (String) args[1].value();
        if (delimiter != null) {
            ramAccounting.addBytes(LIST_ENTRY_OVERHEAD + RamUsageEstimator.sizeOf(delimiter));
        }
        state.values.add(expression);
        state.delimiters.add(delimiter);
        return state;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public StringAggState removeFromAggregatedState(RamAccounting ramAccounting,
                                                    StringAggState previousAggState,
                                                    Input<?>[] stateToRemove) {
        String expression = (String) stateToRemove[0].value();
        String delimiter = (String) stateToRemove[1].value();

        String removed = previousAggState.values.removeFirst();
        assert removed.equals(expression) : "AggregateToWindowFunctionAdapter should always remove the first state";
        removed = previousAggState.delimiters.removeFirst();
        assert Objects.equals(removed, delimiter) : "AggregateToWindowFunctionAdapter should always remove the first state";

        ramAccounting.addBytes(-(LIST_ENTRY_OVERHEAD + RamUsageEstimator.sizeOf(expression)));
        ramAccounting.addBytes(-(LIST_ENTRY_OVERHEAD + RamUsageEstimator.sizeOf(delimiter)));
        return previousAggState;
    }

    @Override
    public StringAggState reduce(RamAccounting ramAccounting, StringAggState state1, StringAggState state2) {
        if (state1.values.isEmpty()) {
            return state2;
        }
        if (state2.values.isEmpty()) {
            return state1;
        }
        state1.values.addAll(state2.values);
        state1.delimiters.addAll(state2.delimiters);
        return state1;
    }

    @Override
    public String terminatePartial(RamAccounting ramAccounting, StringAggState state) {
        if (state.values.isEmpty()) {
            return null;
        } else {
            var sb = new StringBuilder();
            for (int i = 0; i < state.values.size(); i++) {
                sb.append(state.values.get(i));
                if (i < state.values.size() - 1) {
                    int delimiterIndex = i + 1;
                    sb.append(state.delimiters.get(delimiterIndex) == null ? "" : state.delimiters.get(delimiterIndex));
                }
            }
            return sb.toString();
        }
    }

    @Override
    public DataType<?> partialType() {
        return StringAggStateType.INSTANCE;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }
}
