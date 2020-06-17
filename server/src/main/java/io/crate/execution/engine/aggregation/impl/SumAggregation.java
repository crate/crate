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
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;

public class SumAggregation<T extends Number> extends AggregationFunction<T, T> {

    public static final String NAME = "sum";

    public static void register(AggregationImplModule mod) {
        BinaryOperator<Long> add = Math::addExact;
        BinaryOperator<Long> sub = Math::subtractExact;

        mod.register(
            Signature.aggregate(
                NAME,
                DataTypes.FLOAT.getTypeSignature(),
                DataTypes.FLOAT.getTypeSignature()),
            (signature, args) ->
                new SumAggregation<>(DataTypes.FLOAT, Float::sum, (n1, n2) -> n1 - n2, signature)
        );
        mod.register(
            Signature.aggregate(
                NAME,
                DataTypes.DOUBLE.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()),
            (signature, args) ->
                new SumAggregation<>(DataTypes.DOUBLE, Double::sum, (n1, n2) -> n1 - n2, signature)
        );

        for (var supportedType : List.of(DataTypes.BYTE, DataTypes.SHORT, DataTypes.INTEGER, DataTypes.LONG)) {
            mod.register(
                Signature.aggregate(
                    NAME,
                    supportedType.getTypeSignature(),
                    DataTypes.LONG.getTypeSignature()),
                (signature, args) ->
                    new SumAggregation<>(supportedType, DataTypes.LONG, add, sub, signature)
            );
        }
    }

    private final FunctionInfo info;
    private final Signature signature;
    private final BinaryOperator<T> addition;
    private final BinaryOperator<T> subtraction;
    private final DataType<T> returnType;
    private final int bytesSize;

    @VisibleForTesting
    private SumAggregation(final DataType<T> returnType,
                           final BinaryOperator<T> addition,
                           final BinaryOperator<T> subtraction,
                           Signature signature) {
        this(returnType, returnType, addition, subtraction, signature);
    }

    private SumAggregation(final DataType<?> inputType,
                           final DataType<T> returnType,
                           final BinaryOperator<T> addition,
                           final BinaryOperator<T> subtraction,
                           Signature signature) {
        this.addition = addition;
        this.subtraction = subtraction;
        this.returnType = returnType;

        if (returnType == DataTypes.FLOAT) {
            bytesSize = DataTypes.FLOAT.fixedSize();
        } else if (returnType == DataTypes.DOUBLE) {
            bytesSize = DataTypes.DOUBLE.fixedSize();
        } else {
            bytesSize = DataTypes.LONG.fixedSize();
        }

        this.info = new FunctionInfo(
            new FunctionIdent(NAME, Collections.singletonList(inputType)),
            returnType,
            FunctionInfo.Type.AGGREGATE
        );
        this.signature = signature;
    }

    @Nullable
    @Override
    public T newState(RamAccounting ramAccounting,
                      Version indexVersionCreated,
                      Version minNodeInCluster,
                      MemoryManager memoryManager) {
        ramAccounting.addBytes(bytesSize);
        return null;
    }

    @Override
    public T iterate(RamAccounting ramAccounting, MemoryManager memoryManager, T state, Input[] args) throws CircuitBreakingException {
        return reduce(ramAccounting, state, returnType.value(args[0].value()));
    }

    @Override
    public T reduce(RamAccounting ramAccounting, T state1, T state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        return addition.apply(state1, state2);
    }

    @Override
    public T terminatePartial(RamAccounting ramAccounting, T state) {
        return state;
    }

    @Override
    public DataType<?> partialType() {
        return info.returnType();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public T removeFromAggregatedState(RamAccounting ramAccounting, T previousAggState, Input[] stateToRemove) {
        return subtraction.apply(previousAggState, returnType.value(stateToRemove[0].value()));
    }
}
