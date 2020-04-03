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

import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetaData;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import javax.annotation.Nullable;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Locale;
import java.util.stream.Collectors;

public class JavaScriptLanguage implements UDFLanguage {

    static final String NAME = "javascript";

    private static final Engine ENGINE = Engine.newBuilder()
        .build();

    private static final HostAccess HOST_ACCESS = HostAccess.newBuilder()
        .allowListAccess(true)
        .allowArrayAccess(true)
        .build();

    @Inject
    public JavaScriptLanguage(UserDefinedFunctionService udfService) {
        udfService.registerLanguage(this);
    }

    public Scalar createFunctionImplementation(UserDefinedFunctionMetaData meta,
                                               Signature signature) throws ScriptException {
        FunctionInfo info = new FunctionInfo(
            new FunctionIdent(meta.schema(), meta.name(), meta.argumentTypes()),
            meta.returnType()
        );
        return new JavaScriptUserDefinedFunction(info, signature, meta.definition());
    }

    @Nullable
    public String validate(UserDefinedFunctionMetaData meta) {
        try {
            resolvePolyglotFunctionValue(meta.name(), meta.definition());
            return null;
        } catch (IllegalArgumentException | IOException | PolyglotException t) {
            return String.format(Locale.ENGLISH, "Invalid JavaScript in function '%s.%s(%s)' AS '%s': %s",
                meta.schema(),
                meta.name(),
                meta.argumentTypes().stream().map(DataType::getName).collect(Collectors.joining(", ")),
                meta.definition(),
                t.getMessage()
            );
        }
    }

    static Value resolvePolyglotFunctionValue(String functionName, String script) throws IOException {
        var context = Context.newBuilder("js")
            .engine(ENGINE)
            .allowHostAccess(HOST_ACCESS)
            .build();
        var source = Source.newBuilder("js", script, functionName).build();
        context.eval(source);
        var polyglotFunctionValue = context.getBindings("js").getMember(functionName);
        if (polyglotFunctionValue == null) {
            throw new IllegalArgumentException(
                "The name of the function signature '" + functionName + "' doesn't match " +
                "the function name in the function definition.");
        }
        return polyglotFunctionValue;
    }

    public String name() {
        return NAME;
    }
}
