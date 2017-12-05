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
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;

import javax.annotation.Nullable;

public abstract class MaximumAggregation extends AggregationFunction<Comparable, Comparable> {

    public static final String NAME = "max";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : DataTypes.PRIMITIVE_TYPES) {
            FunctionInfo functionInfo = new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.of(dataType)), dataType, FunctionInfo.Type.AGGREGATE);

            if (dataType instanceof FixedWidthType) {
                mod.register(new FixedMaximumAggregation(functionInfo));
            } else {
                mod.register(new VariableMaximumAggregation(functionInfo));
            }
        }
    }

    private static class FixedMaximumAggregation extends MaximumAggregation {

        private final int size;

        public FixedMaximumAggregation(FunctionInfo info) {
            super(info);
            size = ((FixedWidthType) partialType()).fixedSize();
        }

        @Nullable
        @Override
        public Comparable newState(RamAccountingContext ramAccountingContext,
                                   Version indexVersionCreated,
                                   BigArrays bigArrays) {
            ramAccountingContext.addBytes(size);
            return null;
        }

        @Override
        public Comparable reduce(RamAccountingContext ramAccountingContext, Comparable state1, Comparable state2) {
            if (state1 == null) {
                return state2;
            }
            if (state2 == null) {
                return state1;
            }
            if (state1.compareTo(state2) < 0) {
                return state2;
            }
            return state1;
        }
    }

    private static class VariableMaximumAggregation extends MaximumAggregation {

        private final SizeEstimator<Object> estimator;

        VariableMaximumAggregation(FunctionInfo info) {
            super(info);
            estimator = SizeEstimatorFactory.create(partialType());
        }

        @Nullable
        @Override
        public Comparable newState(RamAccountingContext ramAccountingContext,
                                   Version indexVersionCreated,
                                   BigArrays bigArrays) {
            return null;
        }

        @Override
        public Comparable reduce(RamAccountingContext ramAccountingContext, Comparable state1, Comparable state2) {
            if (state1 == null) {
                if (state2 != null) {
                    ramAccountingContext.addBytes(estimator.estimateSize(state2));
                }
                return state2;
            }
            if (state2 == null) {
                return state1;
            }
            if (state1.compareTo(state2) < 0) {
                ramAccountingContext.addBytes(estimator.estimateSizeDelta(state1, state2));
                return state2;
            }
            return state1;
        }
    }

    MaximumAggregation(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public DataType partialType() {
        return info().returnType();
    }

    @Override
    public Comparable iterate(RamAccountingContext ramAccountingContext, Comparable state, Input... args) throws CircuitBreakingException {
        Object value = args[0].value();
        return reduce(ramAccountingContext, state, (Comparable) value);
    }

    @Override
    public Comparable terminatePartial(RamAccountingContext ramAccountingContext, Comparable state) {
        return state;
    }
}
