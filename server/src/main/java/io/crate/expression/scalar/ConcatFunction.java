/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public abstract class ConcatFunction extends Scalar<String, String> {

    public static final String NAME = "concat";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new StringConcatFunction(
                    new FunctionInfo(new FunctionIdent(NAME, args), DataTypes.STRING),
                    signature
                )
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            )
                .withVariableArity(),
            (signature, args) ->
                new GenericConcatFunction(
                    new FunctionInfo(new FunctionIdent(NAME, args), DataTypes.STRING),
                    signature
                )
        );

        // concat(array[], array[]) -> same as `array_cat(...)`
        module.register(
            Signature.scalar(
                NAME,
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)"),
                parseTypeSignature("array(E)")
            )
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, args) ->
                new ArrayCatFunction(
                    ArrayCatFunction.createInfo(args, NAME),
                    signature
                )
        );
    }

    private final FunctionInfo functionInfo;
    private final Signature signature;

    ConcatFunction(FunctionInfo functionInfo, Signature signature) {
        this.functionInfo = functionInfo;
        this.signature = signature;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.ofUnchecked(functionInfo.returnType(), evaluate(txnCtx, inputs));
    }

    static class StringConcatFunction extends ConcatFunction {

        StringConcatFunction(FunctionInfo functionInfo, Signature signature) {
            super(functionInfo, signature);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input[] args) {
            String firstArg = (String) args[0].value();
            String secondArg = (String) args[1].value();
            if (firstArg == null) {
                if (secondArg == null) {
                    return "";
                }
                return secondArg;
            }
            if (secondArg == null) {
                return firstArg;
            }
            return firstArg + secondArg;
        }
    }

    private static class GenericConcatFunction extends ConcatFunction {

        GenericConcatFunction(FunctionInfo functionInfo, Signature signature) {
            super(functionInfo, signature);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, Input<String>[] args) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                String value = args[i].value();
                if (value != null) {
                    sb.append(value);
                }
            }
            return sb.toString();
        }
    }
}
