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
import org.apache.lucene.util.BytesRef;
import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class MaximumAggregation<T extends Comparable<T>> extends AggregationFunction<MaximumAggregation.MaximumAggState<T>> {

    public static final String NAME = "max";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType dataType : new DataType[]{DataType.STRING, DataType.IP}) {
            mod.registerAggregateFunction(
                    new MaximumAggregation<BytesRef>(
                            new FunctionInfo(new FunctionIdent(NAME,
                                    ImmutableList.of(dataType)),
                                    dataType, true)) {
                        @Override
                        public MaximumAggState<BytesRef> newState() {
                            return new MaximumAggState<BytesRef>() {
                                @Override
                                public void readFrom(StreamInput in) throws IOException {
                                    if (!in.readBoolean()) {
                                        setValue((BytesRef) DataType.STRING.streamer().readFrom(in));
                                    }
                                }

                                @Override
                                public void writeTo(StreamOutput out) throws IOException {
                                    BytesRef value = (BytesRef) value();
                                    out.writeBoolean(value == null);
                                    if (value != null) {
                                        DataType.STRING.streamer().writeTo(out, value);
                                    }
                                }
                            };
                        }
                    }
            );
        }
        mod.registerAggregateFunction(
                new MaximumAggregation<Double>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.DOUBLE)),
                                DataType.DOUBLE, true)) {
                    @Override
                    public MaximumAggState<Double> newState() {
                        return new MaximumAggState<Double>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue((Double)DataType.DOUBLE.streamer().readFrom(in));
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Object value = value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    DataType.DOUBLE.streamer().writeTo(out, value);
                                }
                            }
                        };
                    }
                }
        );
        mod.registerAggregateFunction(
                new MaximumAggregation<Float>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.FLOAT)),
                                DataType.FLOAT, true)) {
                    @Override
                    public MaximumAggState<Float> newState() {
                        return new MaximumAggState<Float>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue((Float)DataType.FLOAT.streamer().readFrom(in));
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Object value = value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    DataType.FLOAT.streamer().writeTo(out, value);
                                }
                            }
                        };
                    }
                }
        );
        for (final DataType dataType : new DataType[]{DataType.LONG, DataType.TIMESTAMP}) {
            mod.registerAggregateFunction(
                    new MaximumAggregation<Long>(
                            new FunctionInfo(new FunctionIdent(NAME,
                                    ImmutableList.of(dataType)),
                                    dataType, true)) {
                        @Override
                        public MaximumAggState<Long> newState() {
                            return new MaximumAggState<Long>() {
                                @Override
                                public void readFrom(StreamInput in) throws IOException {
                                    if (!in.readBoolean()) {
                                        setValue((Long)dataType.streamer().readFrom(in));
                                    }
                                }

                                @Override
                                public void writeTo(StreamOutput out) throws IOException {
                                    Object value = value();
                                    out.writeBoolean(value == null);
                                    if (value != null) {
                                        dataType.streamer().writeTo(out, value);
                                    }
                                }
                            };
                        }
                    }
            );
        }
        mod.registerAggregateFunction(
                new MaximumAggregation<Short>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.SHORT)),
                                DataType.SHORT, true)) {
                    @Override
                    public MaximumAggState<Short> newState() {
                        return new MaximumAggState<Short>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue((Short)DataType.SHORT.streamer().readFrom(in));
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Object value = value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    DataType.SHORT.streamer().writeTo(out, value);
                                }
                            }
                        };
                    }
                }
        );
        mod.registerAggregateFunction(
                new MaximumAggregation<Integer>(
                        new FunctionInfo(new FunctionIdent(NAME,
                                ImmutableList.of(DataType.INTEGER)),
                                DataType.INTEGER, true)) {
                    @Override
                    public MaximumAggState<Integer> newState() {
                        return new MaximumAggState<Integer>() {
                            @Override
                            public void readFrom(StreamInput in) throws IOException {
                                if (!in.readBoolean()) {
                                    setValue((Integer)DataType.INTEGER.streamer().readFrom(in));
                                }
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                Object value = value();
                                out.writeBoolean(value == null);
                                if (value != null) {
                                    DataType.INTEGER.streamer().writeTo(out, value);
                                }
                            }
                        };
                    }
                }
        );
        
    }

    MaximumAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public boolean iterate(MaximumAggState<T> state, Input... args) {
        state.add((T) args[0].value());
        return true;
    }

    public static abstract class MaximumAggState<T extends Comparable<T>> extends AggregationState<MaximumAggState<T>> {

        private T value = null;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(MaximumAggState<T> other) {
            if (other.value() == null) {
                return;
            } else if (value() == null) {
                value = other.value;
                return;
            }

            if (compareTo(other) < 0) {
                value = other.value;
            }
        }

        void add(T otherValue) {
            if (otherValue == null) {
                return;
            } else if (value() == null) {
                value = otherValue;
                return;
            }

            if (compareValue(otherValue) < 0) {
                value = otherValue;
            }
        }

        public void setValue(T value) {
            this.value = value;
        }

        @Override
        public int compareTo(MaximumAggState<T> o) {
            if (o == null) return -1;
            return compareValue(o.value);
        }

        public int compareValue(T otherValue) {
            if (value == null) return (otherValue == null ? 0 : -1);
            if (otherValue == null) return 1;

            return value.compareTo(otherValue);
        }

        @Override
        public String toString() {
            return "<MaximumAggState \"" + value + "\"";
        }
    }


}
