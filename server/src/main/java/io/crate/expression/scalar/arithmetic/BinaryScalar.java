/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.arithmetic;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;
import java.util.function.BinaryOperator;

public final class BinaryScalar<T> extends Scalar<T, T> {

    private final BinaryOperator<T> func;
    private final FunctionInfo info;
    @Nullable
    private final Signature signature;
    private final DataType<T> type;

    public BinaryScalar(BinaryOperator<T> func, String name, DataType<T> type, Set<FunctionInfo.Feature> feature) {
        this(func, name, null, type, feature);
    }

    public BinaryScalar(BinaryOperator<T> func,
                        String name,
                        @Nullable Signature signature,
                        DataType<T> type,
                        Set<FunctionInfo.Feature> feature) {
        this.func = func;
        this.info = new FunctionInfo(new FunctionIdent(name, Arrays.asList(type, type)), type, FunctionInfo.Type.SCALAR, feature);
        this.signature = signature;
        this.type = type;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public T evaluate(TransactionContext txnCtx, Input<T>[] args) {
        T arg0Value = type.value(args[0].value());
        T arg1Value = type.value(args[1].value());

        if (arg0Value == null) {
            return null;
        }
        if (arg1Value == null) {
            return null;
        }
        try {
            return func.apply(arg0Value, arg1Value);
        } catch (ArithmeticException ae) {
            throw new IllegalArgumentException(ae.getMessage());
        }
    }
}
