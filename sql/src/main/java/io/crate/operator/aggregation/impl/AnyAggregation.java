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

public class AnyAggregation<T extends Comparable<T>> extends AggregationFunction<AnyAggregation.AnyAggState<T>> {

    public static final String NAME = "any";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataType.PRIMITIVE_TYPES) {
            mod.registerAggregateFunction(
                    new AnyAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t)), t, true))
            );
        }
    }

    AnyAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public boolean iterate(AnyAggState<T> state, Input... args) {
        state.add((T) args[0].value());
        return false;
    }

    @Override
    public AnyAggState newState() {
        switch (info.ident().argumentTypes().get(0)) {
            case STRING:
            case IP:
                return new AnyAggState<BytesRef>() {

                    @Override
                    @SuppressWarnings("unchecked")
                    public void readFrom(StreamInput in) throws IOException {
                        if (!in.readBoolean()) {
                            setValue(in.readBytesRef());
                        }
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        BytesRef value = (BytesRef) value();
                        out.writeBoolean(value == null);
                        out.writeBytesRef(value);
                    }
                };
            case DOUBLE:
                return new AnyAggState<Double>() {

                    @Override
                    public void readFrom(StreamInput in) throws IOException {
                        if (!in.readBoolean()) {
                            setValue(in.readDouble());
                        }
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        Double value = (Double) value();
                        out.writeBoolean(value == null);
                        if (value != null) {
                            out.writeDouble(value);
                        }
                    }
                };
            case FLOAT:
                return new AnyAggState<Float>() {

                    @Override
                    public void readFrom(StreamInput in) throws IOException {
                        if (!in.readBoolean()) {
                            setValue(in.readFloat());
                        }
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        Float value = (Float) value();
                        out.writeBoolean(value == null);
                        if (value != null) {
                            out.writeFloat(value);
                        }
                    }
                };
            case LONG:
            case TIMESTAMP:
                return new AnyAggState<Long>() {
                    @Override
                    public void readFrom(StreamInput in) throws IOException {
                        if (!in.readBoolean()) {
                            setValue(in.readLong());
                        }
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        Long value = (Long) value();
                        out.writeBoolean(value == null);
                        if (value != null) {
                            out.writeLong(value);
                        }
                    }
                };
            case SHORT:
                return new AnyAggState<Short>() {
                    @Override
                    public void readFrom(StreamInput in) throws IOException {
                        if (!in.readBoolean()) {
                            setValue(in.readShort());
                        }
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        Short value = (Short) value();
                        out.writeBoolean(value == null);
                        if (value != null) {
                            out.writeShort(value);
                        }
                    }
                };
            case INTEGER:
                return new AnyAggState<Integer>() {
                    @Override
                    public void readFrom(StreamInput in) throws IOException {
                        if (!in.readBoolean()) {
                            setValue(in.readInt());
                        }
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        Integer value = (Integer) value();
                        out.writeBoolean(value == null);
                        if (value != null) {
                            out.writeInt(value);
                        }
                    }
                };
            case BOOLEAN:
                return new AnyAggState<Boolean>() {

                    public void add(String otherValue) {
                        // this is how lucene stores the truth
                        setValue(otherValue.charAt(0) == 'T');
                    }

                    @Override
                    public void readFrom(StreamInput in) throws IOException {
                        setValue(in.readOptionalBoolean());
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        out.writeOptionalBoolean((Boolean)value());
                    }
                };
            default:
                throw new IllegalArgumentException("Illegal ParameterInfo for ANY");
        }
    }


    public static abstract class AnyAggState<T extends Comparable<T>> extends AggregationState<AnyAggState<T>> {

        private T value = null;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(AnyAggState<T> other) {
            this.value = other.value;
        }

        @Override
        public int compareTo(AnyAggState<T> o) {
            if (o == null) return 1;
            if (value == null) return (o.value == null ? 0 : -1);
            if (o.value == null) return 1;

            return 0; // any two object that are not null are considered equal
        }

        public void add(T otherValue) {
            value = otherValue;
        }

        public void setValue(T value) {
            this.value = value;
        }


        @Override
        public String toString() {
            return "<AnyAggState \"" + value + "\"";
        }
    }


}
