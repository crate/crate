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
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
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

public abstract class ArbitraryAggregation<T extends Comparable<T>> extends AggregationFunction<ArbitraryAggregation.ArbitraryAggState<T>> {

    public static final String NAME = "arbitrary";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType t : DataTypes.PRIMITIVE_TYPES) {
            mod.register(new ArbitraryAggregation(
                            new FunctionInfo(
                                    new FunctionIdent(NAME, ImmutableList.of(t)),
                                    t, FunctionInfo.Type.AGGREGATE)
                    ) {
                             @Override
                             public AggregationState newState(RamAccountingContext ramAccountingContext) {
                                 SizeEstimator<Object> sizeEstimator = SizeEstimatorFactory.create(t);
                                 return new ArbitraryAggState(ramAccountingContext, t.streamer());
                             }
                         }
            );
        }
    }

    ArbitraryAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public boolean iterate(ArbitraryAggState<T> state, Input... args) {
        state.add((T) args[0].value());
        return false;
    }


    static class ArbitraryAggState<T extends Comparable<T>> extends AggregationState<ArbitraryAggState<T>> {

        Streamer streamer;
        private Object value = null;

        public ArbitraryAggState(RamAccountingContext ramAccountingContext, Streamer streamer) {
            super(ramAccountingContext);
            this.streamer = streamer;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(ArbitraryAggState<T> other) {
            if (this.value == null){
                setValue(other.value);
            }
        }

        @Override
        public int compareTo(ArbitraryAggState<T> o) {
            if (o == null) return 1;
            if (value == null) return (o.value == null ? 0 : -1);
            if (o.value == null) return 1;

            return 0; // any two object that are not null are considered equal
        }

        public void add(T otherValue) {
            setValue(otherValue);
        }

        public void setValue(Object value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "<AnyAggState \"" + (value) + "\">";
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            setValue(streamer.readValueFrom(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            streamer.writeValueTo(out, value);
        }
    }
}
