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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import io.crate.data.Input;
import io.crate.expression.scalar.object.ObjectMergeFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public abstract class ConcatFunction extends Scalar<String, String> {

    public static final String NAME = "concat";

    public static void register(ScalarFunctions module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            )
            .withFeature(Feature.NON_NULLABLE),
            StringConcatFunction::new
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            )
            .withFeature(Feature.NON_NULLABLE)
            .withVariableArity(),
            GenericConcatFunction::new
        );

        // concat(array[], array[]) -> same as `array_cat(...)`
        module.register(
            Signature.scalar(
                NAME,
                TypeSignature.parse("array(E)"),
                TypeSignature.parse("array(E)"),
                TypeSignature.parse("array(E)")
            )
            .withFeature(Feature.NON_NULLABLE)
            .withTypeVariableConstraints(typeVariable("E")),
            ArrayCatFunction::new
        );

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                DataTypes.UNTYPED_OBJECT.getTypeSignature()
            )
            .withFeature(Feature.NON_NULLABLE),
            ObjectMergeFunction::new
        );
    }

    ConcatFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
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
        return Literal.ofUnchecked(boundSignature.returnType(), evaluate(txnCtx, nodeCtx, inputs));
    }

    static class StringConcatFunction extends ConcatFunction {

        StringConcatFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
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

        public GenericConcatFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
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
