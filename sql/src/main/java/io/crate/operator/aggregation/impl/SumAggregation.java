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
import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SumAggregation extends AggregationFunction<SumAggregation.SumAggState> {

    public static final String NAME = "sum";
    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataType.NUMERIC_TYPES) {
            mod.registerAggregateFunction(
                    new SumAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t)), DataType.DOUBLE, true))
            );
        }

    }

    SumAggregation(FunctionInfo info) {
        this.info = info;
    }

    public static class SumAggState extends AggregationState<SumAggState> {

        private Double value = null; // sum that aggregates nothing returns null, not 0.0

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(SumAggState other) {
            add(other.value);
        }

        public void add(Object value) {
            if (value != null) {
                this.value = (this.value == null ? 0.0 : this.value) + ((Number)value).doubleValue();
            }
        }

        @Override
        public int compareTo(SumAggState o) {
            if (o == null) return 1;
            if (value == null) return o.value == null ? 0 : -1;
            if (o.value == null) return 1;

            return Double.compare(value, o.value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readFrom(StreamInput in) throws IOException {
            if (!in.readBoolean()) {
                value = in.readDouble();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(value == null);
            if (value != null) {
                out.writeDouble(value);
            }
        }
    }


    @Override
    public boolean iterate(SumAggState state, Input... args) {
        state.add(args[0].value());
        return true;
    }

    @Override
    public SumAggState newState() {
        return new SumAggState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
