/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.ArrayList;
import java.util.List;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public final class ArrayAgg extends AggregationFunction<List<Object>, List<Object>> {

    public static final String NAME = "array_agg";
    public static final Signature SIGNATURE =
        Signature.aggregate(
            NAME,
            parseTypeSignature("E"),
            parseTypeSignature("array(E)")
        ).withTypeVariableConstraints(typeVariable("E"));


    public static void register(AggregationImplModule module) {
        module.register(SIGNATURE, ArrayAgg::new);
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final SizeEstimator<Object> sizeEstimator;

    public ArrayAgg(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.sizeEstimator = SizeEstimatorFactory.create(boundSignature.getArgumentDataTypes().get(0));
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
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
        return boundSignature.getReturnType().createType();
    }
}
