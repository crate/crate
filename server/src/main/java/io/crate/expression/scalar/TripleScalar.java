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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;


/**
 * Scalar implementation that wraps another function: f(T,T,T) -> R
 * <br />
 * null values will result in null as output
 */
public final class TripleScalar<R, T> extends Scalar<R, T> {

    private final ThreeParametersFunction<T, T, T, R> func;
    private final DataType<T> type;

    public TripleScalar(Signature signature,
                        BoundSignature boundSignature,
                        DataType<T> type,
                        ThreeParametersFunction<T, T, T, R> func) {
        super(signature, boundSignature);
        assert signature.hasFeature(Feature.NULLABLE) : "A TriScalar is NULLABLE by definition";
        assert boundSignature.argTypes().stream().allMatch(t -> t.id() == type.id()) :
            "All argument types of the bound signature must match the type argument";
        this.type = type;
        this.func = func;
    }

    @SafeVarargs
    @Override
    public final R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>... args) {
        assert args.length == 3 : "TripleScalar expects exactly 3 arguments, got: " + args.length;
        T value1 = type.sanitizeValue(args[0].value());
        if (value1 == null) {
            return null;
        }
        T value2 = type.sanitizeValue(args[1].value());
        if (value2 == null) {
            return null;
        }
        T value3 = type.sanitizeValue(args[2].value());
        if (value3 == null) {
            return null;
        }
        return func.apply(value1, value2, value3);
    }
}
