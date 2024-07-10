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

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

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
import io.crate.statistics.SketchStreamer;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class TopKAggregation extends AggregationFunction<TopKAggregation.TopKState, List<Object>> {

    public static final String NAME = "top_k";

    public static final Signature DEFAULT_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.parse("V"))
            .returnType(DataTypes.UNDEFINED.getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .typeVariableConstraints(typeVariable("V"))
            .build();

    public static final Signature PARAMETER_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.parse("V"),
                DataTypes.INTEGER.getTypeSignature())
            .returnType(DataTypes.UNDEFINED.getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .typeVariableConstraints(typeVariable("V"))
            .build();

    static {
        DataTypes.register(TopKAggregation.TopKStateType.ID, in -> TopKAggregation.TopKStateType.INSTANCE);
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
    public TopKState newState(RamAccounting ramAccounting,
                              Version indexVersionCreated,
                              Version minNodeInCluster,
                              MemoryManager memoryManager) {
        return new TopKState(new ItemsSketch<>(4), 4);
    }

    @Override
    public TopKState iterate(RamAccounting ramAccounting,
                             MemoryManager memoryManager,
                             TopKState state,
                             Input<?>... args) throws CircuitBreakingException {
        if (state.itemsSketch.isEmpty() && args.length == 2) {
            Integer limit = (Integer) args[1].value();
            // ItemsSketch maxMapSize must be an within the  power of 2, like 2, 4 , 8, 16 etc.
            // Hence, we convert the limit to the closest power of 2.
            // So Limit 6 becomes 8, because 8 is 2^3
            int maxMapSize = convertToClosestPowerOfTwo(limit);
            state = new TopKState(new ItemsSketch<>(maxMapSize), limit);
        }
        Object value = args[0].value();
        state.itemsSketch.update(value);
        return state;
    }

    private int convertToClosestPowerOfTwo(int x) {
        return (int) Math.pow(2, Math.ceil(Math.log(x) / Math.log(2)));
    }

    @Override
    public TopKState reduce(RamAccounting ramAccounting, TopKState state1, TopKState state2) {
        return new TopKState(state1.itemsSketch.merge(state2.itemsSketch), Math.min(state1.limit, state2.limit));
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, TopKState state) {
        ItemsSketch.Row<Object>[] frequentItems = state.itemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        int limit = Math.min(frequentItems.length, state.limit);
        var result = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            var item = frequentItems[i];
            result.add(List.of(item.getItem(), item.getEstimate()));
        }
        return result;
    }

    public DataType<?> partialType() {
        return TopKStateType.INSTANCE;
    }

    public record TopKState(ItemsSketch<Object> itemsSketch, int limit) {}

    static final class TopKStateType extends DataType<TopKState> implements Streamer<TopKState> {

        public static final int ID = 4232;
        public static final TopKStateType INSTANCE = new TopKStateType();

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
            return "top_k_state";
        }

        @Override
        public Streamer<TopKState> streamer() {
            return this;
        }

        @Override
        public TopKState sanitizeValue(Object value) {
            return (TopKState) value;
        }

        @Override
        public TopKState readValueFrom(StreamInput in) throws IOException {
            SketchStreamer<Object> streamer = new SketchStreamer<>(DataTypes.UNDEFINED);
            var limit = in.readInt();
            byte[] bytes = in.readByteArray();
            return new TopKState(ItemsSketch.getInstance(Memory.wrap(bytes), streamer), limit);
        }

        @Override
        public void writeValueTo(StreamOutput out, TopKState v) throws IOException {
            out.writeInt(v.limit);
            SketchStreamer<Object> streamer = new SketchStreamer<>(DataTypes.UNDEFINED);
            out.writeByteArray(v.itemsSketch.toByteArray(streamer));
        }

        @Override
        public long valueBytes(TopKState value) {
            throw new UnsupportedOperationException("valueSize is not implemented for TopKStateType");
        }

        @Override
        public int compare(TopKState s1, TopKState s2) {
            return 0;
        }
    }

}
