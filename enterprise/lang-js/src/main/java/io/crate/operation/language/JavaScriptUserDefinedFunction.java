/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.language;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import java.io.IOException;
import java.util.List;

import static io.crate.operation.language.JavaScriptLanguage.resolvePolyglotFunctionValue;

public class JavaScriptUserDefinedFunction extends Scalar<Object, Object> {

    private final FunctionInfo info;
    private final String script;

    JavaScriptUserDefinedFunction(FunctionInfo info, String script) {
        this.info = info;
        this.script = script;
    }

    @Override
    public Scalar<Object, Object> compile(List<Symbol> arguments) {
        try {
            return new CompiledFunction(
                resolvePolyglotFunctionValue(
                    info.ident().name(),
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
    public Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
        try {
            var function = resolvePolyglotFunctionValue(info.ident().name(), script);
            var polyglotValueArgs = PolyglotValuesConverter.toPolyglotValues(args);
            return PolyglotValuesConverter.toCrateObject(
                function.execute(polyglotValueArgs),
                info.returnType());
        } catch (PolyglotException | IOException e) {
            throw new io.crate.exceptions.ScriptException(
                e.getLocalizedMessage(),
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private class CompiledFunction extends Scalar<Object, Object> {

        private final Value function;

        private CompiledFunction(Value function) {
            this.function = function;
        }

        @Override
        public final Object evaluate(TransactionContext txnCtx, Input<Object>[] args) {
            var polyglotValueArgs = PolyglotValuesConverter.toPolyglotValues(args);
            try {
                return toCrateObject(
                    function.execute(polyglotValueArgs),
                    info.returnType());
            } catch (PolyglotException e) {
                throw new io.crate.exceptions.ScriptException(
                    e.getLocalizedMessage(),
                    e,
                    JavaScriptLanguage.NAME
                );
            }
        }

        @Override
        public FunctionInfo info() {
            // Return the functionInfo of the outer class, because the function
            // info is the same for every compiled function instance.
            return info;
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
