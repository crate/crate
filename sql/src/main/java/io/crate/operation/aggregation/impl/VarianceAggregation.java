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
import io.crate.operation.aggregation.statistics.moment.Variance;
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


public class VarianceAggregation extends AggregationFunction<VarianceAggregation.VarianceState, Double> {

    public static final String NAME = "variance";

    static {
        DataTypes.register(VarianceStateType.ID, () -> VarianceStateType.INSTANCE);
    }

    public static void register(AggregationImplModule mod) {
        for (DataType<?> t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            mod.register(new VarianceAggregation(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(t)), DataTypes.DOUBLE,
                FunctionInfo.Type.AGGREGATE)));
        }
        mod.register(new VarianceAggregation(new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.TIMESTAMP)), DataTypes.DOUBLE,
            FunctionInfo.Type.AGGREGATE)));
    }

    public static class VarianceState implements Comparable<VarianceState> {

        private final Variance variance;

        public VarianceState() {
            this.variance = new Variance();
        }

        private void addValue(double val) {
            variance.increment(val);
        }

        private Double value() {
            double result = variance.result();
            return (Double.isNaN(result) ? null : result);
        }

        @Override
        public int compareTo(VarianceState o) {
            return Double.compare(variance.result(), o.variance.result());
        }
    }

    public static class VarianceStateType extends DataType<VarianceState>
        implements Streamer<VarianceState>, FixedWidthType {

        public static final VarianceStateType INSTANCE = new VarianceStateType();
        public static final int ID = 2048;

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
            return "variance_state";
        }

        @Override
        public Streamer<?> streamer() {
            return this;
        }

        @Override
        public VarianceState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (VarianceState) value;
        }

        @Override
        public int compareValueTo(VarianceState val1, VarianceState val2) {
            return val1.compareTo(val2);
        }

        @Override
        public VarianceState readValueFrom(StreamInput in) throws IOException {
            VarianceState state = new VarianceState();
            state.variance.readFrom(in);
            return state;
        }

        @Override
        public void writeValueTo(StreamOutput out, Object v) throws IOException {
            VarianceState state = (VarianceState) v;
            state.variance.writeTo(out);
        }

        @Override
        public int fixedSize() {
            return 56;
        }
    }

    private final FunctionInfo info;

    public VarianceAggregation(FunctionInfo info) {
        this.info = info;
    }


    @Nullable
    @Override
    public VarianceState newState(RamAccountingContext ramAccountingContext,
                                  Version indexVersionCreated,
                                  BigArrays bigArrays) {
        ramAccountingContext.addBytes(VarianceStateType.INSTANCE.fixedSize());
        return new VarianceState();
    }

    @Override
    public VarianceAggregation.VarianceState iterate(RamAccountingContext ramAccountingContext, VarianceAggregation.VarianceState state, Input... args) throws CircuitBreakingException {
        if (state != null) {
            Number value = (Number) args[0].value();
            if (value != null) {
                state.addValue(value.doubleValue());
            }
        }
        return state;
    }

    @Override
    public VarianceAggregation.VarianceState reduce(RamAccountingContext ramAccountingContext, VarianceAggregation.VarianceState state1, VarianceAggregation.VarianceState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.variance.merge(state2.variance);
        return state1;
    }

    @Override
    public Double terminatePartial(RamAccountingContext ramAccountingContext, VarianceAggregation.VarianceState state) {
        return state.value();
    }

    @Override
    public DataType partialType() {
        return VarianceStateType.INSTANCE;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
