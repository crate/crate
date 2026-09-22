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

import java.util.function.BiFunction;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;

public final class BinaryScalar<T1, T2, R> extends Scalar<R, Object> {

    private final BiFunction<T1, T2, R> func;

    public BinaryScalar(BiFunction<T1, T2, R> func,
                        Signature signature,
                        BoundSignature boundSignature) {
        super(signature, boundSignature);
        assert signature.hasFeature(Feature.STRICTNULL) : "A BinaryScalar is NULLABLE by definition";
        this.func = func;
    }

    @Override
    @SafeVarargs
    @SuppressWarnings("unchecked")
    public final R evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object> ... args) {
        T1 arg0 = (T1) args[0].value();
        if (arg0 == null) {
            return null;
        }
        T2 arg1 = (T2) args[1].value();
        if (arg1 == null) {
            return null;
        }
        try {
            return func.apply(arg0, arg1);
        } catch (ArithmeticException ae) {
            throw new IllegalArgumentException(ae.getMessage(), ae);
        }
    }
}
