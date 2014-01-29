/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.AggregationState;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class CountDistinctAggregation extends AggregationFunction<CountDistinctAggregation.CountDistinctAggState> {

    public static final String NAME = "count";
    public static final boolean DISTINCT = true;
    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataType.ALL_TYPES) {
            mod.registerAggregateFunction(
                    new CountDistinctAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t), DISTINCT), DataType.LONG, true))
            );
        }
    }

    CountDistinctAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public boolean iterate(CountDistinctAggState state, Input... args) {
        state.add(args[0].value());
        return true;
    }

    @Override
    public CountDistinctAggState newState() {
        return new CountDistinctAggState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    public static class CountDistinctAggState extends AggregationState<CountDistinctAggState> {

        public Set<Object> seenValues;
        Long value = 0L;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(CountDistinctAggState other) {
        }

        public void add(Object otherValue) {
            if(seenValues.add(otherValue)) {
                value++;
            }
        }

        @Override
        public int compareTo(CountDistinctAggState o) {
            return Long.compare(value, o.value);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                value = in.readVLong();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (value != null) {
                out.writeBoolean(true);
                out.writeVLong(value);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public void setSeenValuesRef(Set<Object> seenValues) {
            this.seenValues = seenValues;
        }
    }
}