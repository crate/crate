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

package io.crate.expression.scalar.conditional;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;

import javax.annotation.Nullable;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.DataTypes.tryFindNotNullType;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class NullIfFunction extends Scalar<Object, Object> {

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("E"),
                parseTypeSignature("E"),
                parseTypeSignature("E")
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new NullIfFunction(FunctionInfo.of(NAME, args, tryFindNotNullType(args)), signature)
        );
    }

    public static final String NAME = "nullif";
    private final FunctionInfo info;
    private final Signature signature;

    private NullIfFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
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
    public Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        Object arg0Value = args[0].value();
        return arg0Value != null && arg0Value.equals(args[1].value()) ? null : arg0Value;
    }
}
