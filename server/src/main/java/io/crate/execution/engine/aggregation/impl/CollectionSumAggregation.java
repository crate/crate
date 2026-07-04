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
 * Wrapper aggregation function that computes SUM over a set of distinct values.
 * Used internally to support {@code SUM(DISTINCT x)} syntax.
 *
 * The SQL parser transforms {@code SUM(DISTINCT x)} into:
 * {@code collection_sum(CollectSetAggregation(x))}
 */
public final class CollectionSumAggregation extends AggregationFunction<Set<Number>, Number> {

    public static final String NAME = "collection_sum";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(io.crate.metadata.Scalar.Feature.DETERMINISTIC)
                .build(),
            CollectionSumAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public CollectionSumAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Set<Number> newState(RamAccounting ramAccounting,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
        return new java.util.HashSet<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Number> iterate(RamAccounting ramAccounting,
                               MemoryManager memoryManager,
                               Set<Number> state,
                               Input<?>... args) throws CircuitBreakingException {
        Object value = args[0].value();
        if (value instanceof Set) {
            state.addAll((Set<Number>) value);
        }
        return state;
    }

    @Override
    public Set<Number> reduce(RamAccounting ramAccounting, Set<Number> state1, Set<Number> state2) {
        state1.addAll(state2);
        return state1;
    }

    @Override
    public Number terminatePartial(RamAccounting ramAccounting, Set<Number> state) {
        long sum = 0;
        for (Number n : state) {
            sum += n.longValue();
        }
        return sum;
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
