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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Locale;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariableOfAnyType;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class FormatFunction extends Scalar<String, Object> {

    public static final String NAME = "format";

    public static final Signature SIGNATURE =
        Signature.scalar(
            NAME,
            DataTypes.STRING.getTypeSignature(),
            parseTypeSignature("E"),
            DataTypes.STRING.getTypeSignature()
        )
            .withTypeVariableConstraints(typeVariableOfAnyType("E"))
            .withVariableArity();


    public static void register(ScalarFunctionModule module) {
        module.register(
            SIGNATURE,
            (signature, argumentTypes) ->
                new FormatFunction(
                    new FunctionInfo(new FunctionIdent(NAME, argumentTypes), DataTypes.STRING),
                    signature
                )

        );
    }

    private final FunctionInfo info;
    private final Signature signature;

    private FormatFunction(FunctionInfo info, Signature signature) {
        this.info = info;
        this.signature = signature;
    }

    @SafeVarargs
    @Override
    public final String evaluate(TransactionContext txnCtx, Input<Object>... args) {
        assert args.length > 1 : "number of args must be > 1";
        Object arg0Value = args[0].value();
        assert arg0Value != null : "1st argument must not be null";

        Object[] values = new Object[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            Object value = args[i + 1].value();
            values[i] = value;
        }

        return String.format(Locale.ENGLISH, (String) arg0Value, values);
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
}
