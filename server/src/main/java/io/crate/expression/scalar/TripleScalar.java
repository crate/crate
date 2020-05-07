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
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;

import javax.annotation.Nullable;


/**
 * Scalar implementation that wraps another function: f(T,T,T) -> R
 * <br />
 * null values will result in null as output
 */
public final class TripleScalar<R, T> extends Scalar<R, T> {

    private final FunctionInfo info;
    @Nullable
    private final Signature signature;
    private final ThreeParametersFunction<T, T, T, R> func;

    public TripleScalar(FunctionInfo info, ThreeParametersFunction<T, T, T, R> func) {
        this(info, null, func);
    }

    public TripleScalar(FunctionInfo info,
                        @Nullable Signature signature,
                        ThreeParametersFunction<T, T, T, R> func) {
        this.info = info;
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
        assert args.length == 3 : "TripleScalar expects exactly 3 arguments, got: " + args.length;
        T value1 = args[0].value();
        if (value1 == null) {
            return null;
        }
        T value2 = args[1].value();
        if (value2 == null) {
            return null;
        }
        T value3 = args[2].value();
        if (value3 == null) {
            return null;
        }
        return func.apply(value1, value2, value3);
    }
}
