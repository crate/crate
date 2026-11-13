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
import java.util.Map;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;
import io.crate.types.TimestampType;

/// `collect_set` for element types whose values are `Long`, backed by a primitive [LongHashSet].
public final class LongCollectSetAggregation extends AggregationFunction<LongHashSet, List<Object>> {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LongHashSet.class);

    static {
        DataTypes.register(LongHashSetStateType.ID, _ -> LongHashSetStateType.INSTANCE);
    }

    static boolean supports(DataType<?> elementType) {
        int id = elementType.id();
        return id == LongType.ID || id == TimestampType.ID_WITH_TZ || id == TimestampType.ID_WITHOUT_TZ;
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    LongCollectSetAggregation(Signature signature, BoundSignature boundSignature) {
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
    public AggregationFunction<Map<Object, Long>, List<Object>> optimizeForExecutionAsWindowFunction(Version minNodeVersion) {
        return new CollectSetAggregation.RemovableCumulativeCollectSet(signature, boundSignature);
    }

    @Override
    public LongHashSet newState(RamAccounting ramAccounting,
                                Version minNodeInCluster,
                                MemoryManager memoryManager) {
        LongHashSet state = new LongHashSet();
        ramAccounting.addBytes(SHALLOW_SIZE + RamUsageEstimator.sizeOf(state.keys));
        return state;
    }

    @Override
    public LongHashSet iterate(RamAccounting ramAccounting,
                               MemoryManager memoryManager,
                               LongHashSet state,
                               Input<?>... args) {
        Object value = args[0].value();
        if (value == null) {
            return state;
        }
        int capacityBefore = state.keys.length;
        state.add((long) value);
        accountForGrowth(ramAccounting, state, capacityBefore);
        return state;
    }

    @Override
    public LongHashSet reduce(RamAccounting ramAccounting, LongHashSet state1, LongHashSet state2) {
        int capacityBefore = state1.keys.length;
        state1.addAll(state2);
        accountForGrowth(ramAccounting, state1, capacityBefore);
        return state1;
    }

    private static void accountForGrowth(RamAccounting ramAccounting, LongHashSet state, int capacityBefore) {
        int capacityAfter = state.keys.length;
        if (capacityAfter != capacityBefore) {
            ramAccounting.addBytes((long) (capacityAfter - capacityBefore) * Long.BYTES);
        }
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, LongHashSet state) {
        ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(ArrayType.ARRAY_LIST_SHALLOW_SIZE));
        ArrayList<Object> values = new ArrayList<>(state.size());
        for (LongCursor cursor : state) {
            values.add(cursor.value);
        }
        return values;
    }

    @Override
    public DataType<?> partialType() {
        return LongHashSetStateType.INSTANCE;
    }

    static final class LongHashSetStateType extends DataType<LongHashSet> implements Streamer<LongHashSet> {

        static final int ID = 1030;
        static final LongHashSetStateType INSTANCE = new LongHashSetStateType();

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
            return "long_hash_set";
        }

        @Override
        public Streamer<LongHashSet> streamer() {
            return this;
        }

        @Override
        public LongHashSet sanitizeValue(Object value) {
            return (LongHashSet) value;
        }

        @Override
        public int compare(LongHashSet val1, LongHashSet val2) {
            return 0;
        }

        @Override
        public LongHashSet readValueFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            LongHashSet values = new LongHashSet(size);
            for (int i = 0; i < size; i++) {
                values.add(in.readLong());
            }
            return values;
        }

        @Override
        public void writeValueTo(StreamOutput out, LongHashSet values) throws IOException {
            out.writeVInt(values.size());
            for (LongCursor cursor : values) {
                out.writeLong(cursor.value);
            }
        }

        @Override
        public long valueBytes(LongHashSet values) {
            return values == null ? 0 : SHALLOW_SIZE + RamUsageEstimator.sizeOf(values.keys);
        }
    }
}
