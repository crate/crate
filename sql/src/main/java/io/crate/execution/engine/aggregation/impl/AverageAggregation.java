/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class AverageAggregation extends AggregationFunction<AverageAggregation.AverageState, Double> {

    public static final String[] NAMES = new String[]{"avg", "mean"};
    public static final String NAME = NAMES[0];
    private final FunctionInfo info;

    static {
        DataTypes.register(AverageStateType.ID, in -> AverageStateType.INSTANCE);
    }

    /**
     * register as "avg" and "mean"
     */
    public static void register(AggregationImplModule mod) {
        for (String name : NAMES) {
            for (DataType<?> t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
                mod.register(new AverageAggregation(new FunctionInfo(
                    new FunctionIdent(name, ImmutableList.<DataType>of(t)), DataTypes.DOUBLE,
                    FunctionInfo.Type.AGGREGATE)));
            }
            mod.register(new AverageAggregation(new FunctionInfo(
                new FunctionIdent(name, List.of(DataTypes.TIMESTAMPZ)), DataTypes.DOUBLE,
                FunctionInfo.Type.AGGREGATE)));
        }
    }

    public static class AverageState implements Comparable<AverageState> {

        private double sum = 0;
        private long count = 0;

        public Double value() {
            if (count > 0) {
                return sum / count;
            } else {
                return null;
            }
        }

        @Override
        public int compareTo(AverageState o) {
            if (o == null) {
                return 1;
            } else {
                int compare = Double.compare(sum, o.sum);
                if (compare == 0) {
                    return Long.compare(count, o.count);
                }
                return compare;
            }
        }

        @Override
        public String toString() {
            return "sum: " + sum + " count: " + count;
        }
    }

    public static class AverageStateType extends DataType<AverageState>
        implements FixedWidthType, Streamer<AverageState> {

        public static final int ID = 1024;
        private static final AverageStateType INSTANCE = new AverageStateType();
        private static final int AVERAGE_STATE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(AverageState.class);

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
            return "average_state";
        }

        @Override
        public Streamer<AverageState> streamer() {
            return this;
        }

        @Override
        public AverageState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (AverageState) value;
        }

        @Override
        public int compare(AverageState val1, AverageState val2) {
            if (val1 == null) return -1;
            return val1.compareTo(val2);
        }

        @Override
        public AverageState readValueFrom(StreamInput in) throws IOException {
            AverageState averageState = new AverageState();
            averageState.sum = in.readDouble();
            averageState.count = in.readVLong();
            return averageState;
        }

        @Override
        public void writeValueTo(StreamOutput out, AverageState v) throws IOException {
            out.writeDouble(v.sum);
            out.writeVLong(v.count);
        }

        @Override
        public int fixedSize() {
            return AVERAGE_STATE_SIZE;
        }
    }

    AverageAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public AverageState iterate(RamAccounting ramAccounting,
                                MemoryManager memoryManager,
                                AverageState state,
                                Input... args) {
        if (state != null) {
            Number value = (Number) args[0].value();
            if (value != null) {
                state.count++;
                state.sum += value.doubleValue();
            }
        }
        return state;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public AverageState removeFromAggregatedState(RamAccounting ramAccounting,
                                                  AverageState previousAggState,
                                                  Input[] stateToRemove) {
        if (previousAggState != null) {
            Number value = (Number) stateToRemove[0].value();
            if (value != null) {
                previousAggState.count--;
                previousAggState.sum -= value.doubleValue();
            }
        }
        return previousAggState;
    }

    @Override
    public AverageState reduce(RamAccounting ramAccounting, AverageState state1, AverageState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.count += state2.count;
        state1.sum += state2.sum;
        return state1;
    }

    @Override
    public Double terminatePartial(RamAccounting ramAccounting, AverageState state) {
        return state.value();
    }

    @Nullable
    @Override
    public AverageState newState(RamAccounting ramAccounting,
                                 Version indexVersionCreated,
                                 Version minNodeInCluster,
                                 MemoryManager memoryManager) {
        ramAccounting.addBytes(AverageStateType.INSTANCE.fixedSize());
        return new AverageState();
    }

    @Override
    public DataType partialType() {
        return AverageStateType.INSTANCE;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
