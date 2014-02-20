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
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class CollectSetAggregation<T extends Comparable<T>> extends AggregationFunction<CollectSetAggregation.CollectSetAggState<T>> {

    public static final String NAME = "collect_set";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : new DataType[]{DataType.STRING, DataType.IP}) {

            mod.registerAggregateFunction(
                    new CollectSetAggregation<BytesRef>(
                            new FunctionInfo(new FunctionIdent(NAME,
                                    ImmutableList.of(dataType)),
                                    dataType, true)
                    ) {
                        @Override
                        public CollectSetAggState<BytesRef> newState() {
                            return new CollectSetAggState<BytesRef>() {
                                @Override
                                public void readFrom(StreamInput in) throws IOException {
                                    setValue(DataType.STRING_SET.streamer().readFrom(in));
                                }

                                @Override
                                public void writeTo(StreamOutput out) throws IOException {
                                    DataType.STRING_SET.streamer().writeTo(out, value());
                                }
                            };
                        }
                    }
            );
        }
        mod.registerAggregateFunction(
                new CollectSetAggregation<Double>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.DOUBLE)),
                                DataType.DOUBLE, true)
                ) {
                    @Override
                    public CollectSetAggState<Double> newState() {
                        return new CollectSetAggState<Double>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                setValue(DataType.DOUBLE_SET.streamer().readFrom(in));
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                DataType.DOUBLE_SET.streamer().writeTo(out, value());
                            }
                        };
                    }
                }
        );
        mod.registerAggregateFunction(
                new CollectSetAggregation<Float>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.FLOAT)),
                                DataType.FLOAT, true)
                ) {
                    @Override
                    public CollectSetAggState<Float> newState() {
                        return new CollectSetAggState<Float>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                setValue(DataType.FLOAT_SET.streamer().readFrom(in));
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                DataType.FLOAT_SET.streamer().writeTo(out, value());
                            }
                        };
                    }
                }
        );
        for (final DataType dataType : new DataType[]{DataType.LONG, DataType.TIMESTAMP}) {
            mod.registerAggregateFunction(
                    new CollectSetAggregation<Long>(
                            new FunctionInfo(new FunctionIdent(NAME,
                                    ImmutableList.of(dataType)),
                                    dataType, true)
                    ) {
                        @Override
                        public CollectSetAggState<Long> newState() {
                            return new CollectSetAggState<Long>() {
                                @Override
                                public void readFrom(StreamInput in) throws IOException {
                                    setValue(dataType.streamer().readFrom(in));
                                }

                                @Override
                                public void writeTo(StreamOutput out) throws IOException {
                                    dataType.streamer().writeTo(out, value());
                                }
                            };
                        }
                    }
            );
        }
        mod.registerAggregateFunction(
                new CollectSetAggregation<Short>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.SHORT)),
                                DataType.SHORT, true)
                ) {
                    @Override
                    public CollectSetAggState<Short> newState() {
                        return new CollectSetAggState<Short>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                setValue(DataType.SHORT_SET.streamer().readFrom(in));
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                DataType.SHORT_SET.streamer().writeTo(out, value());
                            }
                        };
                    }
                }
        );
        mod.registerAggregateFunction(
                new CollectSetAggregation<Integer>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.INTEGER)),
                                DataType.INTEGER, true)
                ) {
                    @Override
                    public CollectSetAggState<Integer> newState() {
                        return new CollectSetAggState<Integer>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                setValue(DataType.INTEGER_SET.streamer().readFrom(in));
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                DataType.INTEGER_SET.streamer().writeTo(out, value());
                            }
                        };
                    }
                }
        );
        mod.registerAggregateFunction(
                new CollectSetAggregation<Boolean>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.BOOLEAN)),
                                DataType.BOOLEAN, true)
                ) {
                    @Override
                    public CollectSetAggState<Boolean> newState() {
                        return new CollectSetAggState<Boolean>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                setValue(DataType.BOOLEAN_SET.streamer().readFrom(in));
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                DataType.BOOLEAN_SET.streamer().writeTo(out, value());
                            }
                        };
                    }
                }
        );
    }


    CollectSetAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public boolean iterate(CollectSetAggState<T> state, Input... args) {
        state.add((T) args[0].value());
        return true;
    }

    public static abstract class CollectSetAggState<T> extends AggregationState<CollectSetAggState<T>> {

        private Set<T> value = new HashSet<>();

        @Override
        public Set<T> value() {
            return value;
        }

        @Override
        public void reduce(CollectSetAggState<T> other) {
            value.addAll(other.value());
        }

        void add(T otherValue) {
            value.add(otherValue);
        }

        public void setValue(Object value) {
            this.value = (Set<T>)value;
        }

        @Override
        public int compareTo(CollectSetAggState<T> o) {
            if (o == null) return -1;
            return compareValue(o.value);
        }

        public int compareValue(Set<T> otherValue) {
            return value.size() < otherValue.size() ? -1 : value.size() == otherValue.size() ? 0 : 1;
        }

        @Override
        public String toString() {
            return "<CollectSetAggState \"" + value + "\"";
        }
    }


}
