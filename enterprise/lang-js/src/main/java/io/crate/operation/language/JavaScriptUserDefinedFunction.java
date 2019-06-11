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
import io.crate.types.ArrayType;
import io.crate.types.GeoPointType;
import io.crate.types.ObjectType;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaScriptUserDefinedFunction extends Scalar<Object, Object> {

    private final FunctionInfo info;
    private final String script;

    JavaScriptUserDefinedFunction(FunctionInfo info, String script) {
        this.info = info;
        this.script = script;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Scalar<Object, Object> compile(List<Symbol> arguments) {
        try {
            return new CompiledFunction(JavaScriptLanguage.bindScript(script));
        } catch (ScriptException e) {
            // this should not happen if the script was evaluated upfront
            throw new io.crate.exceptions.ScriptException(
                "compile error",
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    @Override
    public Object evaluate(TransactionContext txnCtx, Input<Object>[] values) {
        try {
            return evaluateScriptWithBindings(JavaScriptLanguage.bindScript(script), values);
        } catch (ScriptException e) {
            // this should not happen if the script was evaluated upfront
            throw new io.crate.exceptions.ScriptException(
                "evaluation error",
                e,
                JavaScriptLanguage.NAME
            );
        }
    }

    private class CompiledFunction extends Scalar<Object, Object> {

        private final Bindings bindings;

        private CompiledFunction(Bindings bindings) {
            this.bindings = bindings;
        }

        @Override
        public FunctionInfo info() {
            // return the functionInfo of the outer class,
            // because the function info is the same for every compiled instance of a function
            return info;
        }

        @Override
        public final Object evaluate(TransactionContext txnCtx, Input<Object>[] values) {
            return evaluateScriptWithBindings(bindings, values);
        }

    }

    private Object evaluateScriptWithBindings(Bindings bindings, Input<Object>[] values) {
        Object[] args = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            args[i] = values[i].value();
        }

        Object result;
        try {
            result = ((ScriptObjectMirror) bindings.get(info.ident().name())).call(this, args);
        } catch (NullPointerException e) {
            throw new io.crate.exceptions.ScriptException(
                "The name of the function signature doesn't match the function name in the function definition.",
                JavaScriptLanguage.NAME
            );
        } catch (Exception e) {
            throw new io.crate.exceptions.ScriptException(
                e.getMessage(),
                e,
                JavaScriptLanguage.NAME
            );
        }

        if (result instanceof ScriptObjectMirror) {
            return info.returnType().value(convertScriptResult((ScriptObjectMirror) result));
        } else if (result != null && "Undefined".equals(result.getClass().getSimpleName())) {
            return null;
        } else {
            return info.returnType().value(result);
        }
    }

    private Object convertScriptResult(ScriptObjectMirror scriptObject) {
        switch (info.returnType().id()) {
            case ArrayType.ID:
                if (scriptObject.isArray()) {
                    return scriptObject.values().toArray();
                }
                break;
            case ObjectType.ID:
                return scriptObject.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            case GeoPointType.ID:
                if (scriptObject.isArray()) {
                    return GeoPointType.INSTANCE.value(scriptObject.values().toArray());
                }
                break;
            default:
                return scriptObject.values().toArray();
        }
        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
            "The return type of the function [%s] is not compatible with the type of the function evaluation result.",
            info.returnType()));
    }
}
