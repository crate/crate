/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.statistics.moment.StandardDeviation;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;

import javax.annotation.Nullable;
import java.io.IOException;

public class StandardDeviationAggregation extends AggregationFunction<StandardDeviationAggregation.StdDevState, Double> {

    public static final String NAME = "stddev";

    static {
        DataTypes.register(StdDevStateType.ID, () -> StdDevStateType.INSTANCE);
    }

    public static void register(AggregationImplModule mod) {
        for (DataType<?> t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            mod.register(new StandardDeviationAggregation(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(t)), DataTypes.DOUBLE,
                FunctionInfo.Type.AGGREGATE)));
        }
        mod.register(new StandardDeviationAggregation(new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.TIMESTAMP)), DataTypes.DOUBLE,
            FunctionInfo.Type.AGGREGATE)));
    }

    public static class StdDevState implements Comparable<StdDevState> {

        private final StandardDeviation stdDev;

        public StdDevState() {
            this.stdDev = new StandardDeviation();
        }

        private void addValue(double val) {
            this.stdDev.increment(val);
        }

        private Double value() {
            double result = stdDev.result();
            return (Double.isNaN(result) ? null : result);
        }

        @Override
        public int compareTo(StdDevState o) {
            return Double.compare(stdDev.result(), o.stdDev.result());
        }
    }

    public static class StdDevStateType extends DataType<StdDevState>
        implements Streamer<StdDevState>, FixedWidthType {

        public static final StdDevStateType INSTANCE = new StdDevStateType();
        public static final int ID = 8192;

        @Override
        public int id() {
            return ID;
        }

        @Override
        public Precedence precedence() {
            return Precedence.Custom;
        }

        @Override
        public String getName() {
            return "stddev_state";
        }

        @Override
        public Streamer<?> streamer() {
            return this;
        }

        @Override
        public StdDevState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (StdDevState) value;
        }

        @Override
        public int compareValueTo(StdDevState val1, StdDevState val2) {
            return val1.compareTo(val2);
        }

        @Override
        public int fixedSize() {
            return 56;
        }

        @Override
        public StdDevState readValueFrom(StreamInput in) throws IOException {
            StdDevState state = new StdDevState();
            state.stdDev.readFrom(in);
            return state;
        }

        @Override
        public void writeValueTo(StreamOutput out, Object v) throws IOException {
            StdDevState state = (StdDevState) v;
            state.stdDev.writeTo(out);
        }
    }

    private final FunctionInfo info;

    public StandardDeviationAggregation(FunctionInfo functionInfo) {
        this.info = functionInfo;
    }

    @Nullable
    @Override
    public StdDevState newState(RamAccountingContext ramAccountingContext,
                                Version indexVersionCreated,
                                BigArrays bigArrays) {
        ramAccountingContext.addBytes(StdDevStateType.INSTANCE.fixedSize());
        return new StdDevState();
    }

    @Override
    public StdDevState iterate(RamAccountingContext ramAccountingContext, StdDevState state, Input... args) throws CircuitBreakingException {
        if (state != null) {
            Number value = (Number) args[0].value();
            if (value != null) {
                state.addValue(value.doubleValue());
            }
        }
        return state;
    }

    @Override
    public StdDevState reduce(RamAccountingContext ramAccountingContext, StdDevState state1, StdDevState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.stdDev.merge(state2.stdDev);
        return state1;
    }

    @Override
    public Double terminatePartial(RamAccountingContext ramAccountingContext, StdDevState state) {
        return state.value();
    }

    @Override
    public DataType partialType() {
        return StdDevStateType.INSTANCE;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
