/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.Input;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.HashSet;
import java.util.Set;

public class CollectSetAggregation extends AggregationFunction<Set<Object>, Set<Object>> {

    public static final String NAME = "collect_set";
    private final SizeEstimator<Object> innerTypeEstimator;

    private FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (final DataType dataType : DataTypes.PRIMITIVE_TYPES) {
            mod.register(new CollectSetAggregation(new FunctionInfo(new FunctionIdent(NAME,
                    ImmutableList.of(dataType)),
                    new SetType(dataType), FunctionInfo.Type.AGGREGATE)));
        }
    }

    CollectSetAggregation(FunctionInfo info) {
        this.innerTypeEstimator = SizeEstimatorFactory.create(((SetType) info.returnType()).innerType());
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Set<Object> iterate(RamAccountingContext ramAccountingContext, Set<Object> state, Input... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (value == null) {
            return state;
        }
        if (state.add(value)) {
            ramAccountingContext.addBytes(innerTypeEstimator.estimateSize(value));
        }
        return state;
    }

    @Override
    public Set<Object> newState(RamAccountingContext ramAccountingContext) {
        ramAccountingContext.addBytes(36L); // overhead for the HashSet (map ref 8 + 28 for fields inside the map)
        return new HashSet<>();
    }

    @Override
    public DataType partialType() {
        return info.returnType();
    }

    @Override
    public Set<Object> reduce(RamAccountingContext ramAccountingContext, Set<Object> state1, Set<Object> state2) {
        for (Object newValue : state2) {
            if (state1.add(newValue)) {
                ramAccountingContext.addBytes(innerTypeEstimator.estimateSize(newValue));
            }
        }
        return state1;
    }

    @Override
    public Set<Object> terminatePartial(RamAccountingContext ramAccountingContext, Set<Object> state) {
        return state;
    }
}
