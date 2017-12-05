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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;

import javax.annotation.Nullable;
import java.io.IOException;

public class AverageAggregation extends AggregationFunction<AverageAggregation.AverageState, Double> {

    public static final String[] NAMES = new String[]{"avg", "mean"};
    public static final String NAME = NAMES[0];
    private final FunctionInfo info;

    static {
        DataTypes.register(AverageStateType.ID, () -> AverageStateType.INSTANCE);
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
                new FunctionIdent(name, ImmutableList.<DataType>of(DataTypes.TIMESTAMP)), DataTypes.DOUBLE,
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

        @Override
        public int id() {
            return ID;
        }

        @Override
        public Precedence precedence() {
            return Precedence.Custom;
        }

        @Override
        public String getName() {
            return "average_state";
        }

        @Override
        public Streamer<?> streamer() {
            return this;
        }

        @Override
        public AverageState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (AverageState) value;
        }

        @Override
        public int compareValueTo(AverageState val1, AverageState val2) {
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
        public void writeValueTo(StreamOutput out, Object v) throws IOException {
            AverageState averageState = (AverageState) v;
            out.writeDouble(averageState.sum);
            out.writeVLong(averageState.count);
        }

        @Override
        public int fixedSize() {
            return DataTypes.LONG.fixedSize() + DataTypes.DOUBLE.fixedSize();
        }
    }

    AverageAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public AverageState iterate(RamAccountingContext ramAccountingContext, AverageState state, Input... args) {
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
    public AverageState reduce(RamAccountingContext ramAccountingContext, AverageState state1, AverageState state2) {
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
    public Double terminatePartial(RamAccountingContext ramAccountingContext, AverageState state) {
        return state.value();
    }

    @Nullable
    @Override
    public AverageState newState(RamAccountingContext ramAccountingContext,
                                 Version indexVersionCreated,
                                 BigArrays bigArrays) {
        ramAccountingContext.addBytes(AverageStateType.INSTANCE.fixedSize());
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
