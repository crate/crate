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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * Wrapper aggregation function that computes ARRAY_AGG over a set of distinct values.
 * Used internally to support {@code ARRAY_AGG(DISTINCT x)} syntax.
 *
 * The SQL parser transforms {@code ARRAY_AGG(DISTINCT x)} into:
 * {@code collection_array_agg(CollectSetAggregation(x))}
 */
public final class CollectionArrayAggAggregation extends AggregationFunction<Set<Object>, List<Object>> {

    public static final String NAME = "collection_array_agg";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(io.crate.metadata.Scalar.Feature.DETERMINISTIC)
                .build(),
            CollectionArrayAggAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public CollectionArrayAggAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Set<Object> newState(RamAccounting ramAccounting,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
        return new java.util.LinkedHashSet<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Object> iterate(RamAccounting ramAccounting,
                               MemoryManager memoryManager,
                               Set<Object> state,
                               Input<?>... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (value instanceof Set) {
            state.addAll((Set<Object>) value);
        }
        return state;
    }

    @Override
    public Set<Object> reduce(RamAccounting ramAccounting, Set<Object> state1, Set<Object> state2) {
        state1.addAll(state2);
        return state1;
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, Set<Object> state) {
        return new ArrayList<>(state);
    }

    @Override
    public DataType<?> partialType() {
        return boundSignature.returnType();
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }
}
