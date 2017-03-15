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

package io.crate.operation.udf;

import io.crate.analyze.FunctionArgumentDefinition;
import io.crate.metadata.FunctionIdent;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import java.util.Locale;
import java.util.stream.Collectors;

public class UserDefinedFunctionFactory {

    public static UserDefinedFunction of(UserDefinedFunctionMetaData meta) {
        switch (meta.language.toLowerCase()) {
            case "javascript":
                try {
                    CompiledScript compiledScript = ((Compilable) JavaScriptUserDefinedFunction.ENGINE)
                        .compile(meta.definition);
                    FunctionIdent ident = new FunctionIdent(
                        meta.schema(),
                        meta.name(),
                        meta.arguments().stream().map(FunctionArgumentDefinition::type).collect(Collectors.toList()));
                    return new JavaScriptUserDefinedFunction(ident, meta.returnType, compiledScript);
                } catch (ScriptException e) {
                    throw new IllegalArgumentException(String.format("Cannot compile the script. [%s]", e));
                }
            default:
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "[%s] language is not supported.", meta.language));
        }
    }
}
