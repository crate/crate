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
 * Wrapper aggregation function that computes STRING_AGG over a set of distinct values.
 * Used internally to support {@code STRING_AGG(DISTINCT x, d)} syntax.
 *
 * The SQL parser transforms {@code STRING_AGG(DISTINCT x, d)} into:
 * {@code collection_string_agg(CollectSetAggregation(x), d)}
 */
public final class CollectionStringAggAggregation extends AggregationFunction<Set<String>, String> {

    public static final String NAME = "collection_string_agg";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                               DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(io.crate.metadata.Scalar.Feature.DETERMINISTIC)
                .build(),
            CollectionStringAggAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public CollectionStringAggAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Set<String> newState(RamAccounting ramAccounting,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
        return new java.util.LinkedHashSet<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> iterate(RamAccounting ramAccounting,
                               MemoryManager memoryManager,
                               Set<String> state,
                               Input<?>... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (value instanceof Set) {
            state.addAll((Set<String>) value);
        }
        return state;
    }

    @Override
    public Set<String> reduce(RamAccounting ramAccounting, Set<String> state1, Set<String> state2) {
        state1.addAll(state2);
        return state1;
    }

    @Override
    public String terminatePartial(RamAccounting ramAccounting, Set<String> state) {
        if (state.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : state) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(s);
            first = false;
        }
        return sb.toString();
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
