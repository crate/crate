/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.symbol;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public enum AggregateMode {
    ITER_PARTIAL {
        @Override
        public DataType returnType(AggregationFunction<?, ?> function) {
            return function.partialType();
        }

        @Override
        public <TP, TF> TF finishCollect(RamAccounting ramAccounting, AggregationFunction<TP, TF> function, TP state) {
            return (TF) state;
        }
    },
    ITER_FINAL,
    PARTIAL_FINAL;

    private static final List<AggregateMode> VALUES = ImmutableList.copyOf(values());

    public DataType returnType(AggregationFunction<?, ?> function) {
        return function.info().returnType();
    }

    public <TP, TF> TF finishCollect(RamAccounting ramAccounting, AggregationFunction<TP, TF> function, TP state) {
        return function.terminatePartial(ramAccounting, state);
    }

    public static void writeTo(AggregateMode mode, StreamOutput out) throws IOException {
        out.writeVInt(mode.ordinal());
    }

    public static AggregateMode readFrom(StreamInput in) throws IOException {
        return VALUES.get(in.readVInt());
    }
}
