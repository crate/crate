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

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.Collections;

public class SumAggregation extends AggregationFunction<Double, Double> {

    public static final String NAME = "sum";

    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            mod.register(new SumAggregation(t));
        }
    }

    @VisibleForTesting
    public SumAggregation(DataType inputType) {
        this.info = new FunctionInfo(
            new FunctionIdent(NAME, Collections.singletonList(inputType)),
            DataTypes.DOUBLE,
            FunctionInfo.Type.AGGREGATE
        );
    }

    @Override
    public Double iterate(RamAccountingContext ramAccountingContext, Double state, Input... args) throws CircuitBreakingException {
        return reduce(ramAccountingContext, state, DataTypes.DOUBLE.value(args[0].value()));
    }

    @Override
    public Double reduce(RamAccountingContext ramAccountingContext, Double state1, Double state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        return state1 + state2;
    }

    @Override
    public Double terminatePartial(RamAccountingContext ramAccountingContext, Double state) {
        return state;
    }

    @Override
    public Double newState(RamAccountingContext ramAccountingContext) {
        ramAccountingContext.addBytes(DataTypes.DOUBLE.fixedSize());
        return null;
    }

    @Override
    public DataType partialType() {
        return info.returnType();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
