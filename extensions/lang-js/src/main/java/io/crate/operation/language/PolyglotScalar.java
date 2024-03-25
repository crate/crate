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

package io.crate.operation.language;


import java.io.IOException;
import java.util.List;

import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;

public final class PolyglotScalar extends Scalar<Object, Object> {

    private final String script;
    private final String graalLanguageId;

    PolyglotScalar(Signature signature,
                   BoundSignature boundSignature,
                   String graalLanguageId,
                   String script) {
        super(signature, boundSignature);
        this.graalLanguageId = graalLanguageId;
        this.script = script;
    }

    @Override
    public Scalar<Object, Object> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        try {
            String functionName = signature.getName().name();
            return new CompiledFunction(
                graalLanguageId,
                signature,
                boundSignature,
                PolyglotLanguage.getFunctionValue(graalLanguageId, functionName, script));
        } catch (PolyglotException | IOException e) {
            // this should not happen if the script was validated upfront
            throw new io.crate.exceptions.ScriptException(
                "compile error",
                e,
                graalLanguageId
            );
        }
    }

    @Override
    @SafeVarargs
    public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object> ... args) {
        try {
            String functionName = signature.getName().name();
            var function = PolyglotLanguage.getFunctionValue(graalLanguageId, functionName, script);
            Object[] values = PolyglotValues.toPolyglotValues(args, boundSignature.argTypes());
            return PolyglotValues.toCrateObject(
                function.execute(values),
                boundSignature.returnType()
            );
        } catch (PolyglotException | IOException e) {
            throw new io.crate.exceptions.ScriptException(
                e.getLocalizedMessage(),
                e,
                graalLanguageId
            );
        }
    }

    private static class CompiledFunction extends Scalar<Object, Object> {

        private final String language;
        private final Value function;

        private CompiledFunction(String language,
                                 Signature signature,
                                 BoundSignature boundSignature,
                                 Value function) {
            super(signature, boundSignature);
            this.language = language;
            this.function = function;
        }

        @Override
        @SafeVarargs
        public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object> ... args) {
            Object[] values = PolyglotValues.toPolyglotValues(args, boundSignature.argTypes());
            try {
                return PolyglotValues.toCrateObject(function.execute(values), boundSignature.returnType());
            } catch (PolyglotException e) {
                throw new io.crate.exceptions.ScriptException(
                    e.getLocalizedMessage(),
                    e,
                    language
                );
            }
        }
    }
}
