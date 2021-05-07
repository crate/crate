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

package io.crate.expression.scalar.arithmetic;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

import java.util.function.BinaryOperator;

public final class BinaryScalar<T> extends Scalar<T, T> {

    private final BinaryOperator<T> func;
    private final Signature signature;
    private final Signature boundSignature;
    private final DataType<T> type;

    public BinaryScalar(BinaryOperator<T> func,
                        Signature signature,
                        Signature boundSignature,
                        DataType<T> type) {
        assert boundSignature.getArgumentDataTypes().stream().allMatch(t -> t.id() == type.id()) :
            "All bound argument types of the signature must match the type argument";
        this.func = func;
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.type = type;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public T evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>[] args) {
        T arg0Value = type.sanitizeValue(args[0].value());
        T arg1Value = type.sanitizeValue(args[1].value());

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
