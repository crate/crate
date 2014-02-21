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
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.LongLiteral;
import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CountAggregation extends AggregationFunction<CountAggregation.CountAggState> {

    public static final String NAME = "count";
    private final FunctionInfo info;

    private static final FunctionInfo COUNT_STAR_FUNCTION = new FunctionInfo(new FunctionIdent(NAME, ImmutableList.<DataType>of()), DataType.LONG, true);

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataType.ALL_TYPES_INC_NULL) {
            mod.registerAggregateFunction(
                    new CountAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t)), DataType.LONG, true))
            );
        }
        // Register function for 0 inputs (count(*))
        mod.registerAggregateFunction(new CountAggregation(COUNT_STAR_FUNCTION));

    }

    CountAggregation(FunctionInfo info) {
        this.info = info;
    }

    public static class CountAggState extends AggregationState<CountAggState> {

        public long value = 0;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(CountAggState other) {
            value += other.value;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            value = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(value);
        }

        @Override
        public String toString() {
            return "CountAggState {" + value + "}";
        }

        @Override
        public int compareTo(CountAggState o) {
            return Long.compare(value, o.value);
        }
    }


    @Override
    public boolean iterate(CountAggState state, Input... args) {
        state.value++;
        return true;
    }

    @Override
    public CountAggState newState() {
        return new CountAggState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        assert (function.arguments().size() <= 1);

        if (function.arguments().size() == 1) {
            if (function.arguments().get(0).symbolType().isLiteral()) {
                if (((Literal)function.arguments().get(0)).valueType() == DataType.NULL) {
                    return new LongLiteral(0);
                } else{
                    return new Function(COUNT_STAR_FUNCTION, ImmutableList.<Symbol>of());
                }
            }
        }
        return function;
    }
}
