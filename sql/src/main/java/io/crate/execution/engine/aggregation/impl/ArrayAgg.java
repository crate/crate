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

package io.crate.execution.engine.aggregation.impl;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionInfo.Type;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;

public final class ArrayAgg extends AggregationFunction<List<Object>, List<Object>> {

    private static final String NAME = "array_agg";

    public static void register(AggregationImplModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams.SINGLE_ANY) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> types) throws IllegalArgumentException {
                var type = types.get(0);
                return new ArrayAgg(type);
            }
        });
    }

    private final FunctionInfo info;
    private final SizeEstimator<Object> sizeEstimator;

    public ArrayAgg(DataType<?> argType) {
        this.sizeEstimator = SizeEstimatorFactory.create(argType);
        this.info = new FunctionInfo(
            new FunctionIdent(NAME, List.of(argType)),
            argType,
            Type.AGGREGATE
        );
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public List<Object> newState(RamAccounting ramAccounting,
                                 Version indexVersionCreated,
                                 Version minNodeInCluster,
                                 MemoryManager memoryManager) {
        return new ArrayList<>();
    }

    @Override
    public List<Object> iterate(RamAccounting ramAccounting,
                                MemoryManager memoryManager,
                                List<Object> state,
                                Input... args) throws CircuitBreakingException {
        var value = args[0].value();
        ramAccounting.addBytes(sizeEstimator.estimateSize(value));
        state.add(value);
        return state;
    }

    @Override
    public List<Object> reduce(RamAccounting ramAccounting, List<Object> state1, List<Object> state2) {
        state1.addAll(state2);
        return state1;
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, List<Object> state) {
        return state;
    }

    @Override
    public DataType<?> partialType() {
        return info().returnType();
    }
}
