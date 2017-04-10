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

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.types.DataType;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import javax.script.*;
import java.util.Locale;
import java.util.stream.Collectors;

public class JavaScriptLanguage implements UDFLanguage {

    static final String NAME = "javascript";

    private static final NashornScriptEngine ENGINE = (NashornScriptEngine) new NashornScriptEngineFactory()
        .getScriptEngine("--no-java", "--no-syntax-extensions");

    @Inject
    public JavaScriptLanguage(UserDefinedFunctionService udfService) {
        udfService.registerLanguage(this);
    }

    public Scalar createFunctionImplementation(UserDefinedFunctionMetaData meta) throws ScriptException {
        FunctionInfo info = new FunctionInfo(
            new FunctionIdent(meta.schema(), meta.name(), meta.argumentTypes()),
            meta.returnType()
        );
        return new JavaScriptUserDefinedFunction(info, meta.definition());
    }

    @Nullable
    public String validate(UserDefinedFunctionMetaData meta) {
        try {
            bindScript(meta.definition());
        } catch (ScriptException e) {
            return String.format(Locale.ENGLISH, "Invalid JavaScript in function '%s.%s(%s)': %s",
                meta.schema(),
                meta.name(),
                meta.argumentTypes().stream().map(DataType::getName).collect(Collectors.joining(", ")),
                e.getMessage()
            );
        }
        return null;
    }

    static Bindings bindScript(String source) throws ScriptException {
        Bindings bindings = ENGINE.createBindings();
        CompiledScript compiledScript = ENGINE.compile(source);
        compiledScript.eval(bindings);
        return bindings;
    }

    public String name() {
        return NAME;
    }
}
