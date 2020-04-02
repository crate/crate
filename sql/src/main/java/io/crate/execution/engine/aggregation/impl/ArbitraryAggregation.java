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

package io.crate.execution.engine.aggregation.impl;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import io.crate.breaker.SizeEstimatorFactory;
import io.crate.data.Input;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;

import javax.annotation.Nullable;

public class ArbitraryAggregation extends AggregationFunction<Object, Object> {

    public static final String NAME = "arbitrary";

    private final FunctionInfo info;
    private final SizeEstimator<Object> partialEstimator;

    public static void register(AggregationImplModule mod) {
        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    supportedType.getTypeSignature()),
                args -> new ArbitraryAggregation(
                    new FunctionInfo(
                        new FunctionIdent(NAME, args),
                        args.get(0),
                        FunctionInfo.Type.AGGREGATE))
            );
        }
    }

    ArbitraryAggregation(FunctionInfo info) {
        this.info = info;
        partialEstimator = SizeEstimatorFactory.create(partialType());
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public DataType<?> partialType() {
        return info.returnType();
    }

    @Nullable
    @Override
    public Object newState(RamAccounting ramAccounting,
                           Version indexVersionCreated,
                           Version minNodeInCluster,
                           MemoryManager memoryManager) {
        return null;
    }

    @Override
    public Object iterate(RamAccounting ramAccounting,
                          MemoryManager memoryManager,
                          Object state,
                          Input... args) {
        return reduce(ramAccounting, state, args[0].value());
    }

    @Override
    public Object reduce(RamAccounting ramAccounting, Object state1, Object state2) {
        if (state1 == null) {
            if (state2 != null) {
                // this case happens only once per aggregation so ram usage is only estimated once
                ramAccounting.addBytes(partialEstimator.estimateSize(state2));
            }
            return state2;
        }
        return state1;
    }

    @Override
    public Object terminatePartial(RamAccounting ramAccounting, Object state) {
        return state;
    }
}
