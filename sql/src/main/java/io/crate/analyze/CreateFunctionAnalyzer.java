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

package io.crate.analyze;

import io.crate.metadata.Schemas;
import io.crate.sql.tree.CreateFunction;

import java.util.List;

import static io.crate.analyze.FunctionArgumentDefinition.toFunctionArgumentDefinitions;

public class CreateFunctionAnalyzer {

    public CreateFunctionAnalyzedStatement analyze(CreateFunction node, Analysis context) {

        List<String> parts = node.name().getParts();
        return new CreateFunctionAnalyzedStatement(
            resolveSchemaName(parts, context.sessionContext().defaultSchema()),
            resolveFunctionName(parts),
            node.replace(),
            toFunctionArgumentDefinitions(node.arguments()),
            DataTypeAnalyzer.convert(node.returnType()),
            node.language(),
            node.definition()
        );
    }

    public static String resolveFunctionName(List<String> parts) {
        return parts.size() == 1 ? parts.get(0) : parts.get(1);
    }

    public static String resolveSchemaName(List<String> parts, String defaultSchema) {
        if (parts.size() == 1) {
            if (defaultSchema != null) {
                return defaultSchema;
            }
            return Schemas.DEFAULT_SCHEMA_NAME;
        }
        return parts.get(0);
    }
}
