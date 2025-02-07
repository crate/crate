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

import java.math.BigDecimal;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

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
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class NumericMinAggregation extends AggregationFunction<BigDecimal, BigDecimal> {

    public static final String NAME = "min";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.AGGREGATE)
        .argumentTypes(DataTypes.NUMERIC.getTypeSignature())
        .returnType(DataTypes.NUMERIC.getTypeSignature())
        .features(Scalar.Feature.DETERMINISTIC)
        .build();
    private static final long INIT_BIG_DECIMAL_SIZE = NumericType.size(BigDecimal.ZERO);

    public static void register(Functions.Builder builder) {
        builder.add(SIGNATURE, NumericMinAggregation::new);
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<BigDecimal> returnType;

    @VisibleForTesting
    private NumericMinAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        // We want to preserve the scale and precision from the
        // numeric argument type for the return type. So we use
        // the incoming numeric type as return type instead of
        // the return type from the signature `sum(count::numeric(16, 2))`
        // should return the type `numeric(16, 2)` not `numeric`
        var argumentType = boundSignature.argTypes().get(0);
        assert argumentType.id() == DataTypes.NUMERIC.id();
        //noinspection unchecked
        this.returnType = (DataType<BigDecimal>) argumentType;
    }

    @Nullable
    @Override
    public BigDecimal newState(RamAccounting ramAccounting,
                               Version indexVersionCreated,
                               Version minNodeInCluster,
                               MemoryManager memoryManager) {
        ramAccounting.addBytes(INIT_BIG_DECIMAL_SIZE);
        return null;
    }

    @Override
    public BigDecimal iterate(RamAccounting ramAccounting,
                              MemoryManager memoryManager,
                              BigDecimal state,
                              Input<?>... args) throws CircuitBreakingException {
        BigDecimal value = returnType.implicitCast(args[0].value());
        if (value != null) {
            if (state != null) {
                if (state.compareTo(value) >= 0) {
                    state = value;
                }
            } else {
                state = value;
            }
        }
        return state;
    }

    @Override
    public BigDecimal reduce(RamAccounting ramAccounting,
                             BigDecimal state1,
                             BigDecimal state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        if (state1.compareTo(state2) < 0) {
            return state1;
        } else {
            return state2;
        }
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, BigDecimal state) {
        if (state != null) {
            ramAccounting.addBytes(NumericType.size(state));
        }
        return state;
    }

    @Override
    public DataType<?> partialType() {
        return returnType;
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
