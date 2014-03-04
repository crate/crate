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
import org.cratedb.DataType;
import org.cratedb.Streamer;
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
        return new AnyAggState(info.ident().argumentTypes().get(0).streamer());
    }


    public static class AnyAggState<T extends Comparable<T>> extends AggregationState<AnyAggState<T>> {


        Streamer<T> streamer;
        private T value = null;

        AnyAggState(Streamer<T> streamer) {
            this.streamer = streamer;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(AnyAggState<T> other) {
            if (this.value == null){
                this.value = other.value;
            }
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
            return "<AnyAggState \"" + (value) + "\">";
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            setValue(streamer.readFrom(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            streamer.writeTo(out, value);
        }
    }


}
