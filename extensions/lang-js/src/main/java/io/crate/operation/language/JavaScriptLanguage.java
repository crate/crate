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

import io.crate.expression.udf.UDFLanguage;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Inject;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

import org.jetbrains.annotations.Nullable;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Locale;
import java.util.stream.Collectors;

public class JavaScriptLanguage implements UDFLanguage {

    static final String NAME = "javascript";

    private static final Engine ENGINE = Engine.newBuilder()
        .option("js.foreign-object-prototype", "true")
        .option("engine.WarnInterpreterOnly", "false")
        .build();

    private static final HostAccess HOST_ACCESS = HostAccess.newBuilder()
        .allowListAccess(true)
        .allowArrayAccess(true)
        .allowMapAccess(true)
        .build();

    @Inject
    public JavaScriptLanguage(UserDefinedFunctionService udfService) {
        udfService.registerLanguage(this);
    }

    public Scalar<?, ?> createFunctionImplementation(UserDefinedFunctionMetadata meta,
                                                     Signature signature,
                                                     BoundSignature boundSignature) throws ScriptException {
        return new JavaScriptUserDefinedFunction(signature, boundSignature, meta.definition());
    }

    @Nullable
    public String validate(UserDefinedFunctionMetadata meta) {
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
