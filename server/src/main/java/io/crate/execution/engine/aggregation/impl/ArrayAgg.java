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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public final class ArrayAgg extends AggregationFunction<List<Object>, List<Object>> {

    public static final String NAME = "array_agg";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.AGGREGATE)
        .argumentTypes(TypeSignature.parse("E"))
        .returnType(TypeSignature.parse("array(E)"))
        .features(Scalar.Feature.DETERMINISTIC)
        .typeVariableConstraints(typeVariable("E"))
        .build();


    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, ArrayAgg::new);
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<Object> elementType;

    @SuppressWarnings("unchecked")
    public ArrayAgg(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.elementType = (DataType<Object>) boundSignature.argTypes().get(0);
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
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
                                Input<?>... args) throws CircuitBreakingException {
        var value = args[0].value();
        ramAccounting.addBytes(elementType.valueBytes(value));
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
        return boundSignature.returnType();
    }
}
