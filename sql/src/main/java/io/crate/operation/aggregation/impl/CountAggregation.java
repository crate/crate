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
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

public class CountAggregation extends AggregationFunction<Long, Long> {

    public static final String NAME = "count";
    private final FunctionInfo info;
    private final boolean hasArgs;

    public static final FunctionInfo COUNT_STAR_FUNCTION = new FunctionInfo(new FunctionIdent(NAME,
            ImmutableList.<DataType>of()), DataTypes.LONG, FunctionInfo.Type.AGGREGATE);

    public static void register(AggregationImplModule mod) {
        mod.register(NAME, new CountAggregationFunctionResolver());
    }

    static class CountAggregationFunctionResolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() == 0) {
                return new CountAggregation(COUNT_STAR_FUNCTION, false);
            } else {
                return new CountAggregation(
                        new FunctionInfo(new FunctionIdent(NAME, dataTypes),
                                DataTypes.LONG, FunctionInfo.Type.AGGREGATE), true);
            }
        }
    }

    CountAggregation(FunctionInfo info, boolean hasArgs) {
        this.info = info;
        this.hasArgs = hasArgs;
    }

    @Override
    public Long iterate(RamAccountingContext ramAccountingContext, Long state, Input... args) {
        if (!hasArgs || args[0].value() != null){
            return state + 1;
        }
        return state;
    }

    @Override
    public Long newState(RamAccountingContext ramAccountingContext) {
        ramAccountingContext.addBytes(DataTypes.LONG.fixedSize());
        return 0L;
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
                if ((function.arguments().get(0)).valueType() == DataTypes.UNDEFINED) {
                    return Literal.newLiteral(0L);
                } else{
                    return new Function(COUNT_STAR_FUNCTION, ImmutableList.<Symbol>of());
                }
            }
        }
        return function;
    }

    @Override
    public DataType partialType() {
        return DataTypes.LONG;
    }

    @Override
    public Long reduce(RamAccountingContext ramAccountingContext, Long state1, Long state2) {
        return state1 + state2;
    }

    @Override
    public Long terminatePartial(RamAccountingContext ramAccountingContext, Long state) {
        return state;
    }
}
