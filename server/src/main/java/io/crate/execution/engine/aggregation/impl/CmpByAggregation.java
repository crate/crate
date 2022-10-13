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

import static io.crate.types.TypeSignature.parseTypeSignature;

import java.util.List;

import javax.annotation.Nullable;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public final class CmpByAggregation extends AggregationFunction<CmpByAggregation.CompareBy, Object> {

    public static final String MAX_BY = "max_by";
    public static final String FIRST = "first";
    public static final String MIN_BY = "min_by";
    public static final String LAST = "last";

    static class CompareBy {

        Comparable<Object> cmpValue;
        Object resultValue;
    }

    static {
        DataTypes.register(CompareByType.ID, CompareByType::new);
    }

    public static void register(AggregationImplModule mod) {
        TypeSignature returnValueType = parseTypeSignature("A");
        TypeSignature cmpType = parseTypeSignature("B");
        var variableConstraintA = TypeVariableConstraint.typeVariableOfAnyType("A");
        var variableConstraintB = TypeVariableConstraint.typeVariableOfAnyType("B");
        for (String name : List.of(MAX_BY, FIRST)) {
            mod.register(
                Signature.aggregate(
                    name,
                    returnValueType,
                    cmpType,
                    returnValueType
                ).withTypeVariableConstraints(
                    variableConstraintA,
                    variableConstraintB
                ),
                (signature, boundSignature) -> new CmpByAggregation(1, signature, boundSignature)
            );
        }
        for (String name : List.of(MIN_BY, LAST)) {
            mod.register(
                Signature.aggregate(
                    name,
                    returnValueType,
                    cmpType,
                    returnValueType
                ).withTypeVariableConstraints(
                    variableConstraintA,
                    variableConstraintB
                ),
                (signature, boundSignature) -> new CmpByAggregation(-1, signature, boundSignature)
            );
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final CompareByType partialType;
    private final int cmpResult;

    public CmpByAggregation(int cmpResult, Signature signature, BoundSignature boundSignature) {
        this.cmpResult = cmpResult;
        this.signature = signature;
        this.boundSignature = boundSignature;
        int size = boundSignature.argTypes().size();
        if (size == 1) {
            this.partialType = (CompareByType) boundSignature.argTypes().get(0);
        } else {
            this.partialType = new CompareByType(
                boundSignature.argTypes().get(0),
                boundSignature.argTypes().get(1)
            );
        }
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
    @Nullable
    public CompareBy newState(RamAccounting ramAccounting,
                              Version indexVersionCreated,
                              Version minNodeInCluster,
                              MemoryManager memoryManager) {
        return new CompareBy();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompareBy iterate(RamAccounting ramAccounting,
                             MemoryManager memoryManager,
                             CompareBy state,
                             Input... args) throws CircuitBreakingException {
        Object cmpVal = args[1].value();

        if (cmpVal instanceof Comparable comparable) {
            Comparable<Object> currentValue = state.cmpValue;
            if (currentValue == null || comparable.compareTo(currentValue) == cmpResult) {
                state.cmpValue = (Comparable<Object>) comparable;
                state.resultValue = args[0].value();
            }
        } else if (cmpVal != null) {
            throw new UnsupportedOperationException(
                "Cannot use `" + signature.getName().displayName() + "` on values of type " + boundSignature.argTypes().get(1).getName());
        }
        return state;
    }

    @Override
    public CompareBy reduce(RamAccounting ramAccounting,
                            CompareBy state1,
                            CompareBy state2) {
        if (state1.cmpValue == null) {
            return state2;
        }
        if (state2.cmpValue == null) {
            return state1;
        }
        if (state2.cmpValue.compareTo(state1.cmpValue) == cmpResult) {
            return state2;
        } else {
            return state1;
        }
    }

    @Override
    public Object terminatePartial(RamAccounting ramAccounting, CompareBy state) {
        return state.resultValue;
    }

    @Override
    public DataType<?> partialType() {
        return partialType;
    }
}
