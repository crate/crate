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
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public abstract class Concat_wsFunction extends Scalar<String, String> {

    public static final String NAME = "concat_ws";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ).withVariableArity(),
            StringConcat_wsFunction::new
        );

    }

    private final Signature signature;
    private final Signature boundSignature;

    Concat_wsFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
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
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        if (anyNonLiterals(function.arguments())) {
            return function;
        }
        Input[] inputs = new Input[function.arguments().size()];
        for (int i = 0; i < function.arguments().size(); i++) {
            inputs[i] = ((Input) function.arguments().get(i));
        }
        //noinspection unchecked
        return Literal.ofUnchecked(boundSignature.getReturnType().createType(), evaluate(txnCtx, nodeCtx, inputs));
    }

    static class StringConcat_wsFunction extends Concat_wsFunction{

        StringConcat_wsFunction(Signature signature, Signature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {

            if(args[0].value() == null) {
                throw new NullPointerException("Delimiting argument of the 'concat_ws' function cannot be null.");
          }

            String delimitArg = (String) args[0].value();

            StringBuilder sb = new StringBuilder();

            for (int i = 1; i < args.length; i++) {
                String value = (String) args[i].value();
                if (value != null) {
                    sb.append(value);
                    if(i != args.length-1 && delimitArg != null) {
                        sb.append(delimitArg);
                    }
                }
            }
            //In the case the last argument is NULL
            if(sb.toString().endsWith(",")) {
                return sb.substring(0,sb.length()-1);
            }
            return sb.toString();
        }
    }
}
