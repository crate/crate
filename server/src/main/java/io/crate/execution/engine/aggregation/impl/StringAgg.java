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
import java.util.List;

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
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * string_agg :: text -> text -> text
 * string_agg(expression, delimiter)
 */
public final class StringAgg extends AggregationFunction<StringAgg.StringAggState, String> {

    private static final String NAME = "string_agg";
    public static final Signature SIGNATURE =
            Signature.builder(NAME, FunctionType.AGGREGATE)
                    .argumentTypes(DataTypes.STRING.getTypeSignature(),
                            DataTypes.STRING.getTypeSignature())
                    .returnType(DataTypes.STRING.getTypeSignature())
                    .features(Scalar.Feature.DETERMINISTIC)
                    .build();


    private static final int LIST_ENTRY_OVERHEAD = 32;

    static {
        DataTypes.register(StringAggStateType.INSTANCE.id(), in -> StringAggStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            SIGNATURE,
            StringAgg::new
        );
    }

    static class StringAggState implements Writeable {

        private final List<String> values;
        private String firstDelimiter;

        public StringAggState() {
            values = new ArrayList<>();
        }

        public StringAggState(StreamInput in) throws IOException {
            values = in.readList(StreamInput::readString);
            firstDelimiter = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(values);
            out.writeOptionalString(firstDelimiter);
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
            return "string_list";
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
                                   Version indexVersionCreated,
                                   Version minNodeInCluster,
                                   MemoryManager memoryManager) {
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
            if (state.firstDelimiter == null && state.values.isEmpty()) {
                state.firstDelimiter = delimiter;
            } else {
                ramAccounting.addBytes(LIST_ENTRY_OVERHEAD + RamUsageEstimator.sizeOf(delimiter));
                state.values.add(delimiter);
            }
        }
        state.values.add(expression);
        return state;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public StringAggState removeFromAggregatedState(RamAccounting ramAccounting,
                                                    StringAggState previousAggState,
                                                    Input<?>[]stateToRemove) {
        String expression = (String) stateToRemove[0].value();
        if (expression == null) {
            return previousAggState;
        }
        String delimiter = (String) stateToRemove[1].value();

        int indexOfExpression = previousAggState.values.indexOf(expression);
        if (indexOfExpression > -1) {
            ramAccounting.addBytes(-LIST_ENTRY_OVERHEAD + RamUsageEstimator.sizeOf(expression));
            if (delimiter != null) {
                String elementNextToExpression = previousAggState.values.get(indexOfExpression + 1);
                if (elementNextToExpression.equalsIgnoreCase(delimiter)) {
                    previousAggState.values.remove(indexOfExpression + 1);
                }
            }
            previousAggState.values.remove(indexOfExpression);
        }
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
        if (state2.firstDelimiter != null) {
            state1.values.add(state2.firstDelimiter);
        }
        state1.values.addAll(state2.values);
        return state1;
    }

    @Override
    public String terminatePartial(RamAccounting ramAccounting, StringAggState state) {
        List<String> values = state.values;
        if (values.isEmpty()) {
            return null;
        } else {
            var sb = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                sb.append(values.get(i));
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
