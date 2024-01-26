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

import static io.crate.operation.language.JavaScriptLanguage.resolvePolyglotFunctionValue;

import java.io.IOException;
import java.util.List;

import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.expression.scalar.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public class JavaScriptUserDefinedFunction extends Scalar<Object, Object> {

    private final String script;

    JavaScriptUserDefinedFunction(Signature signature, BoundSignature boundSignature, String script) {
        super(signature, boundSignature);
        this.script = script;
    }

    @Override
    public Scalar<Object, Object> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        try {
            return new CompiledFunction(
                signature,
                boundSignature,
                resolvePolyglotFunctionValue(
                    signature.getName().name(),
                    script));
        } catch (PolyglotException | IOException e) {
            // this should not happen if the script was validated upfront
            throw new io.crate.exceptions.ScriptException(
                "compile error",
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        try {
            var function = resolvePolyglotFunctionValue(signature.getName().name(), script);
            Object[] polyglotValueArgs = PolyglotValuesConverter.toPolyglotValues(
                args,
                Lists.map(signature.getArgumentTypes(), TypeSignature::createType)
            );
            return PolyglotValuesConverter.toCrateObject(
                function.execute(polyglotValueArgs),
                signature.getReturnType().createType());
        } catch (PolyglotException | IOException e) {
            throw new io.crate.exceptions.ScriptException(
                e.getLocalizedMessage(),
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    private static class CompiledFunction extends Scalar<Object, Object> {

        private final Value function;

        private CompiledFunction(Signature signature, BoundSignature boundSignature, Value function) {
            super(signature, boundSignature);
            this.function = function;
        }

        @Override
        public final Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
            Object[] polyglotValueArgs = PolyglotValuesConverter.toPolyglotValues(
                args,
                Lists.map(signature.getArgumentTypes(), TypeSignature::createType)
            );
            try {
                return toCrateObject(
                    function.execute(polyglotValueArgs),
                    signature.getReturnType().createType());
            } catch (PolyglotException e) {
                throw new io.crate.exceptions.ScriptException(
                    e.getLocalizedMessage(),
                    e,
                    JavaScriptLanguage.NAME
                );
            }
        }
    }


    private static Object toCrateObject(Value value, DataType<?> type) {
        if ("undefined".equalsIgnoreCase(value.getClass().getSimpleName())) {
            return null;
        } else {
            return PolyglotValuesConverter.toCrateObject(value, type);
        }
    }
}
