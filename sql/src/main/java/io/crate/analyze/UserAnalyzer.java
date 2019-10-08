/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.CreateUser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.ParameterExpression;

import java.util.function.Function;

public class UserAnalyzer {

    private final Functions functions;

    UserAnalyzer(Functions functions) {
        this.functions = functions;
    }

    public AnalyzedCreateUser analyze(CreateUser<Expression> node,
                                      Function<ParameterExpression, Symbol> convertParamFunction,
                                      CoordinatorTxnCtx txnContext) {
        return new AnalyzedCreateUser(
            node.name(),
            mappedProperties(node.properties(), convertParamFunction, txnContext));
    }

    public AnalyzedAlterUser analyze(AlterUser<Expression> node,
                                     Function<ParameterExpression, Symbol> convertParamFunction,
                                     CoordinatorTxnCtx txnContext) {
        return new AnalyzedAlterUser(
            node.name(),
            mappedProperties(node.properties(), convertParamFunction, txnContext));
    }

    private GenericProperties<Symbol> mappedProperties(GenericProperties<Expression> properties,
                                                              Function<ParameterExpression, Symbol> convertParamFunction,
                                                              CoordinatorTxnCtx txnContext) {
        ExpressionAnalysisContext exprContext = new ExpressionAnalysisContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            txnContext,
            convertParamFunction,
            FieldProvider.UNSUPPORTED,
            null
        );

        return properties.map(x -> expressionAnalyzer.convert(x, exprContext));
    }
}
