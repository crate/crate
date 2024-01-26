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

import java.util.function.Function;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;


/**
 * Scalar implementation that wraps another function: f(T) -> R
 * <br />
 * null values will result in null as output
 */
public class UnaryScalar<R, T> extends Scalar<R, T> {

    private final Function<T, R> func;
    private final DataType<T> type;

    public UnaryScalar(Signature signature,
                       BoundSignature boundSignature,
                       DataType<T> type,
                       Function<T, R> func) {
        super(signature, boundSignature);
        assert signature.hasFeature(Feature.NULLABLE) : "A UnaryScalar is NULLABLE by definition";
        assert boundSignature.argTypes().get(0).id() == type.id() :
            "The bound argument type of the signature must match the type argument";
        this.type = type;
        this.func = func;
    }

    @SafeVarargs
    @Override
    public final R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>... args) {
        assert args.length == 1 : "UnaryScalar expects exactly 1 argument, got: " + args.length;
        T value = type.sanitizeValue(args[0].value());
        if (value == null) {
            return null;
        }
        return func.apply(value);
    }
}
