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

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;
import org.joda.time.Period;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class IntervalSumAggregation extends AggregationFunction<Period, Period> {

    public static final String NAME = "sum";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.aggregate(
                    NAME,
                    DataTypes.INTERVAL.getTypeSignature(),
                    DataTypes.INTERVAL.getTypeSignature())
                .withFeature(Scalar.Feature.DETERMINISTIC),
            IntervalSumAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final int bytesSize;

    @VisibleForTesting
    private IntervalSumAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.bytesSize = DataTypes.INTERVAL.fixedSize();
    }

    @Nullable
    @Override
    public Period newState(RamAccounting ramAccounting,
                      Version indexVersionCreated,
                      Version minNodeInCluster,
                      MemoryManager memoryManager) {
        ramAccounting.addBytes(bytesSize);
        return null;
    }

    @Override
    public Period iterate(RamAccounting ramAccounting,
                          MemoryManager memoryManager,
                          Period state,
                          Input<?> ... args) throws CircuitBreakingException {
        return reduce(ramAccounting, state, DataTypes.INTERVAL.sanitizeValue(args[0].value()));
    }

    @Override
    public Period reduce(RamAccounting ramAccounting, Period state1, Period state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        return state1.plus(state2);
    }

    @Override
    public Period terminatePartial(RamAccounting ramAccounting, Period state) {
        return state;
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

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public Period removeFromAggregatedState(RamAccounting ramAccounting,
                                            Period previousAggState,
                                            Input<?>[] stateToRemove) {
        return previousAggState.minus(DataTypes.INTERVAL.sanitizeValue(stateToRemove[0].value()));
    }
}
