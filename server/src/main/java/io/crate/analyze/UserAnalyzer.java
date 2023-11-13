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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.sql.tree.AlterRole;
import io.crate.sql.tree.CreateRole;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;

public class UserAnalyzer {

    private final NodeContext nodeCtx;

    UserAnalyzer(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedCreateUser analyze(CreateRole node,
                                      ParamTypeHints paramTypeHints,
                                      CoordinatorTxnCtx txnContext) {
        return new AnalyzedCreateUser(
            node.name(),
            mappedProperties(node.properties(), paramTypeHints, txnContext));
    }

    public AnalyzedAlterUser analyze(AlterRole<Expression> node,
                                     ParamTypeHints paramTypeHints,
                                     CoordinatorTxnCtx txnContext) {
        return new AnalyzedAlterUser(
            node.name(),
            mappedProperties(node.properties(), paramTypeHints, txnContext));
    }

    private GenericProperties<Symbol> mappedProperties(GenericProperties<Expression> properties,
                                                       ParamTypeHints paramTypeHints,
                                                       CoordinatorTxnCtx txnContext) {
        ExpressionAnalysisContext exprContext = new ExpressionAnalysisContext(txnContext.sessionSettings());
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            txnContext,
            nodeCtx,
            paramTypeHints,
            FieldProvider.UNSUPPORTED,
            null
        );
        return properties.map(x -> expressionAnalyzer.convert(x, exprContext));
    }
}
