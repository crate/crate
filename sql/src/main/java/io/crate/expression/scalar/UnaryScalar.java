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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;


/**
 * Scalar implementation that wraps another function: f(T) -> R
 * <br />
 * null values will result in null as output
 */
public class UnaryScalar<R, T> extends Scalar<R, T> {

    private final FunctionInfo info;
    private final Function<T, R> func;
    @Nullable
    private final Signature signature;

    public UnaryScalar(FunctionIdent functionIdent, DataType<?> returnType, Function<T, R> func) {
        this(functionIdent, null, returnType, func);
    }

    public UnaryScalar(String name, DataType<?> argType, DataType<?> returnType, Function<T, R> func) {
        this(new FunctionIdent(name, List.of(argType)), returnType, func);
    }

    public UnaryScalar(String name,
                       Signature signature,
                       DataType<?> argType, DataType<?> returnType,
                       Function<T, R> func) {
        this(new FunctionIdent(name, List.of(argType)), signature, returnType, func);
    }

    public UnaryScalar(FunctionIdent functionIdent,
                       @Nullable Signature signature,
                       DataType<?> returnType,
                       Function<T, R> func) {
        this.info = new FunctionInfo(functionIdent, returnType);
        this.signature = signature;
        this.func = func;
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

    @SafeVarargs
    @Override
    public final R evaluate(TransactionContext txnCtx, Input<T>... args) {
        assert args.length == 1 : "UnaryScalar expects exactly 1 argument, got: " + args.length;
        T value = args[0].value();
        if (value == null) {
            return null;
        }
        return func.apply(value);
    }
}
