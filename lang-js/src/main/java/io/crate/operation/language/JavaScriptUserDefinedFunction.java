/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.operation.language;

import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.types.*;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.runtime.ECMAException;
import jdk.nashorn.internal.runtime.Undefined;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.script.*;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaScriptUserDefinedFunction extends Scalar<Object, Object> {

    private final FunctionInfo info;
    private final CompiledScript compiledScript;
    private final DataType returnType;

    JavaScriptUserDefinedFunction(FunctionIdent ident, DataType returnType, CompiledScript compiledScript) {
        this.info = new FunctionInfo(ident, returnType);
        this.returnType = returnType;
        this.compiledScript = compiledScript;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Scalar<Object, Object> compile(List<Symbol> arguments) {
        // A separate Bindings object allow to create an isolated scope for the function.
        Bindings bindings = JavaScriptLanguage.ENGINE.createBindings();
        try {
            compiledScript.eval(bindings);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot evaluate the script. [%s]", e));
        }
        return new CompiledJavaScriptUserDefinedFunction(info().ident(), returnType, bindings);
    }


    private class CompiledJavaScriptUserDefinedFunction extends Scalar<Object, Object> {

        private final FunctionInfo info;
        private final Bindings bindings;

        private CompiledJavaScriptUserDefinedFunction(FunctionIdent ident,
                                                      DataType returnType,
                                                      Bindings bindings) {
            this.info = new FunctionInfo(ident, returnType);
            this.bindings = bindings;
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @Override
        public final Object evaluate(Input<Object>[] values) {
            return evaluateFunctionWithBindings(bindings, values);
        }
    }

    @Override
    public Object evaluate(Input<Object>[] values) {
        Bindings bindings = JavaScriptLanguage.ENGINE.createBindings();
        try {
            compiledScript.eval(bindings);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot evaluate the script. [%s]", e));
        }
        return evaluateFunctionWithBindings(bindings, values);
    }

    private Object evaluateFunctionWithBindings(Bindings bindings, Input<Object>[] values) {
        Object result;
        try {
            Object[] args = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                args[i] = processBytesRefInputIfNeeded(values[i].value());
            }
            result = ((ScriptObjectMirror) bindings.get(info.ident().name())).call(this, args);
        } catch (NullPointerException e) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "The name [%s] of the function signature doesn't match the function name in the function definition.",
                info.ident().name()));
        } catch (ECMAException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "The function definition cannot be evaluated. [%s]", e)
            );
        }

        if (result instanceof ScriptObjectMirror) {
            return returnType.value(parseScriptObject((ScriptObjectMirror) result));
        } else if (result instanceof Undefined) {
            return null;
        } else {
            return returnType.value(result);
        }
    }

    private static Object processBytesRefInputIfNeeded(Object value) {
        if (value instanceof BytesRef) {
            value = BytesRefs.toString(value);
        } else if (value instanceof Map) {
            convertBytesRefToStringInMap((Map<String, Object>) value);
        } else if (value instanceof Object[]) {
            convertBytesRefToStringInList((Object[]) value);
        }
        return value;
    }

    private static void convertBytesRefToStringInMap(Map<String, Object> value) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            Object item = entry.getValue();
            if (item instanceof BytesRef) {
                entry.setValue(BytesRefs.toString(entry.getValue()));
            } else if (item instanceof Object[]) {
                convertBytesRefToStringInList((Object[]) item);
                entry.setValue(item);
            } else if (item instanceof Map) {
                convertBytesRefToStringInMap((Map<String, Object>) entry.getValue());
            }
        }
    }

    private static void convertBytesRefToStringInList(Object[] value) {
        for (int i = 0; i < value.length; i++) {
            Object item = value[i];
            if (item instanceof BytesRef) {
                value[i] = BytesRefs.toString(item);
            } else if (item instanceof Object[]) {
                convertBytesRefToStringInList((Object[]) value[i]);
            }
        }
    }

    private Object parseScriptObject(ScriptObjectMirror scriptObject) {
        switch (returnType.id()) {
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
                    return GeoPointType.INSTANCE.value(scriptObject.values().stream()
                        .toArray(Object[]::new));
                }
                break;
            case SetType.ID:
                return scriptObject.values().stream().collect(Collectors.toSet());
        }
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "The return type of the function [%s]" +
            " is not compatible with the type of the function evaluation result.", returnType));
    }
}
