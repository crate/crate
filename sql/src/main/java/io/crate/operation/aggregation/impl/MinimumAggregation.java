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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.AggregationState;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class MinimumAggregation extends AggregationFunction<MinimumAggregation.MinimumAggState> {

    public static final String NAME = "min";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : DataTypes.PRIMITIVE_TYPES) {
            mod.register(
                    new MinimumAggregation(
                            new FunctionInfo(new FunctionIdent(NAME,
                                    ImmutableList.of(dataType)), dataType, true)
                    ) {
                        @Override
                        public MinimumAggState newState() {
                            return new MinimumAggState() {
                                @Override
                                public void readFrom(StreamInput in) throws IOException {
                                    if (!in.readBoolean()) {
                                        setValue((Comparable) dataType.streamer().readValueFrom(in));
                                    }
                                }

                                @Override
                                public void writeTo(StreamOutput out) throws IOException {
                                    Object value = value();
                                    out.writeBoolean(value == null);
                                    if (value != null) {
                                        dataType.streamer().writeValueTo(out, value);
                                    }
                                }
                            };
                        }
                    }
            );
        }
    }

    MinimumAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public boolean iterate(MinimumAggState state, Input... args) {
        Object value = args[0].value();
        assert value == null || value instanceof Comparable;
        state.add((Comparable) value);
        return true;
    }

    public static abstract class MinimumAggState extends AggregationState<MinimumAggState> {

        private Comparable value = null;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(MinimumAggState other) {
            if (other.value() == null) {
                return;
            } else if (value() == null) {
                value = other.value;
                return;
            }

            if (compareTo(other) > 0) {
                value = other.value;
            }
        }

        void add(Comparable otherValue) {
            if (otherValue == null) {
                return;
            } else if (value() == null) {
                value = otherValue;
                return;
            }

            if (compareValue(otherValue) > 0) {
                value = otherValue;
            }
        }

        public void setValue(Comparable value) {
            this.value = value;
        }

        @Override
        public int compareTo(MinimumAggState o) {
            if (o == null) return -1;
            return compareValue(o.value);
        }

        public int compareValue(Comparable otherValue) {
            if (value == null) return (otherValue == null ? 0 : 1);
            if (otherValue == null) return -1;
            return value.compareTo(otherValue);
        }

        @Override
        public String toString() {
            return "<MinimumAggState \"" + value + "\"";
        }
    }
}
