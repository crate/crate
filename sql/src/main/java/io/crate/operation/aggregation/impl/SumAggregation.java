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

package io.crate.operation.aggregation.impl;

import com.google.common.annotations.VisibleForTesting;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.function.BinaryOperator;

public class SumAggregation<T extends Number> extends AggregationFunction<T, T> {

    public static final String NAME = "sum";

    public static void register(AggregationImplModule mod) {

        final BinaryOperator<Long> longAddImpl = (n1, n2) -> {
            Long r = n1 + n2;
            // Overflow if both arguments have the opposite sign of the result
            if (((n1 ^ r) & (n2 ^ r)) < 0) {
                throw new ArithmeticException("long overflow");
            }
            return r;
        };

        mod.register(new SumAggregation<Float>(DataTypes.FLOAT, (n1, n2) -> n1 + n2));
        mod.register(new SumAggregation<Double>(DataTypes.DOUBLE, (n1, n2) -> n1 + n2));
        mod.register(new SumAggregation<>(DataTypes.BYTE, DataTypes.LONG, longAddImpl));
        mod.register(new SumAggregation<>(DataTypes.SHORT, DataTypes.LONG, longAddImpl));
        mod.register(new SumAggregation<>(DataTypes.INTEGER, DataTypes.LONG, longAddImpl));
        mod.register(new SumAggregation<>(DataTypes.LONG, longAddImpl));
    }

    private final FunctionInfo info;
    private final BinaryOperator<T> addition;
    private final DataType<T> returnType;
    private final int bytesSize;

    @VisibleForTesting
    private SumAggregation(final DataType returnType, final BinaryOperator<T> addition) {
        this(returnType, returnType, addition);
    }

    private SumAggregation(final DataType inputType, final DataType returnType, final BinaryOperator<T> addition) {
        this.addition = addition;
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
    }

    @Nullable
    @Override
    public T newState(RamAccountingContext ramAccountingContext, Version indexVersionCreated, BigArrays bigArrays) {
        ramAccountingContext.addBytes(bytesSize);
        return null;
    }

    @Override
    public T iterate(RamAccountingContext ramAccountingContext, T state, Input[] args) throws CircuitBreakingException {
        return reduce(ramAccountingContext, state, returnType.value(args[0].value()));
    }

    @Override
    public T reduce(RamAccountingContext ramAccountingContext, T state1, T state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        return addition.apply(state1, state2);
    }

    @Override
    public T terminatePartial(RamAccountingContext ramAccountingContext, T state) {
        return state;
    }

    @Override
    public DataType partialType() {
        return info.returnType();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
