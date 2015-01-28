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
import io.crate.exceptions.CircuitBreakingException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;

public abstract class MinimumAggregation extends AggregationFunction<Comparable, Comparable> {

    public static final String NAME = "min";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : DataTypes.PRIMITIVE_TYPES) {
            FunctionInfo functionInfo = new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(dataType)),
                    dataType, FunctionInfo.Type.AGGREGATE);

            if (dataType instanceof FixedWidthType) {
                mod.register(new FixedMinimumAggregation(functionInfo));
            } else {
                mod.register(new VariableMinimumAggregation(functionInfo));
            }
        }
    }

    private static class VariableMinimumAggregation extends MinimumAggregation {

        private final SizeEstimator<Object> estimator;

        VariableMinimumAggregation(FunctionInfo info) {
            super(info);
            estimator = SizeEstimatorFactory.create(partialType());
        }

        @Override
        public Comparable newState(RamAccountingContext ramAccountingContext) {
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
            if (state1.compareTo(state2) > 0) {
                ramAccountingContext.addBytes(estimator.estimateSizeDelta(state1, state2));
                return state2;
            }
            return state1;
        }
    }

    private static class FixedMinimumAggregation extends MinimumAggregation {

        private final int size;

        FixedMinimumAggregation(FunctionInfo info) {
            super(info);
            size = ((FixedWidthType) partialType()).fixedSize();
        }

        @Override
        public Comparable newState(RamAccountingContext ramAccountingContext) {
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
            if (state1.compareTo(state2) > 0) {
                return state2;
            }
            return state1;
        }
    }

    MinimumAggregation(FunctionInfo info) {
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
    public Comparable terminatePartial(RamAccountingContext ramAccountingContext, Comparable state) {
        return state;
    }

    @Override
    public Comparable iterate(RamAccountingContext ramAccountingContext, Comparable state, Input... args) throws CircuitBreakingException {
        return reduce(ramAccountingContext, state, (Comparable) args[0].value());
    }
}
