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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.SearchPath;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.Expression;

import java.util.List;

import static io.crate.analyze.FunctionArgumentDefinition.toFunctionArgumentDefinitions;

public class CreateFunctionAnalyzer {

    private final NodeContext nodeCtx;

    CreateFunctionAnalyzer(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedCreateFunction analyze(CreateFunction<Expression> node,
                                          ParamTypeHints paramTypeHints,
                                          CoordinatorTxnCtx txnCtx,
                                          SearchPath searchPath) {
        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprCtx = new ExpressionAnalysisContext();

        CreateFunction<Symbol> createFunction = node.map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx));

        List<String> parts = createFunction.name().getParts();
        return new AnalyzedCreateFunction(
            resolveSchemaName(parts, searchPath.currentSchema()),
            resolveFunctionName(parts),
            createFunction.replace(),
            toFunctionArgumentDefinitions(createFunction.arguments()),
            DataTypeAnalyzer.convert(createFunction.returnType()),
            createFunction.language(),
            createFunction.definition()
        );
    }

    static String resolveFunctionName(List<String> parts) {
        return parts.size() == 1 ? parts.get(0) : parts.get(1);
    }

    static String resolveSchemaName(List<String> parts, String sessionSchema) {
        return parts.size() == 1 ? sessionSchema : parts.get(0);
    }
}
