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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.AggregationState;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class CollectSetAggregation<T extends Comparable<T>>
        extends AggregationFunction<CollectSetAggregation.CollectSetAggState> {

    public static final String NAME = "collect_set";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : DataTypes.PRIMITIVE_TYPES) {
            final Streamer<?> setStreamer = new SetType(dataType).streamer();

            mod.register(
                    new CollectSetAggregation(
                            new FunctionInfo(new FunctionIdent(NAME,
                                    ImmutableList.of(dataType)),
                                    new SetType(dataType), FunctionInfo.Type.AGGREGATE
                            )
                    ) {
                        @Override
                        public CollectSetAggState newState() {
                            return new CollectSetAggState() {
                                @Override
                                public void readFrom(StreamInput in) throws IOException {
                                    setValue(setStreamer.readValueFrom(in));
                                }

                                @Override
                                public void writeTo(StreamOutput out) throws IOException {
                                    setStreamer.writeValueTo(out, value());
                                }
                            };
                        }
                    }
            );
        }
    }


    CollectSetAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public boolean iterate(CollectSetAggState state, Input... args) {
        state.add(args[0].value());
        return true;
    }

    public static abstract class CollectSetAggState extends AggregationState<CollectSetAggState> {

        private Set<Object> value = new HashSet<>();

        @Override
        public Set value() {
            return value;
        }

        @Override
        public void reduce(CollectSetAggState other) {
            value.addAll(other.value());
        }

        void add(Object otherValue) {
            // ignore null values? yes
            if (otherValue != null) {
                value.add(otherValue);
            }
        }

        public void setValue(Object value) {
            this.value = (Set)value;
        }

        @Override
        public int compareTo(CollectSetAggState o) {
            if (o == null) return -1;
            return compareValue(o.value);
        }

        public int compareValue(Set otherValue) {
            return value.size() < otherValue.size() ? -1 : value.size() == otherValue.size() ? 0 : 1;
        }

        @Override
        public String toString() {
            return "<CollectSetAggState \"" + value + "\"";
        }
    }
}
