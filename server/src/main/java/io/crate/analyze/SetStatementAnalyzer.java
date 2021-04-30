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
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.SetStatement;

class SetStatementAnalyzer {

    private final NodeContext nodeCtx;

    SetStatementAnalyzer(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedStatement analyze(SetStatement<Expression> node,
                                     ParamTypeHints typeHints,
                                     CoordinatorTxnCtx txnCtx) {
        boolean isPersistent = node.settingType().equals(SetStatement.SettingType.PERSISTENT);
        var exprAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            typeHints,
            FieldProvider.FIELDS_AS_LITERAL,
            null
        );
        SetStatement<Symbol> statement = node.map(x -> exprAnalyzer.convert(x, new ExpressionAnalysisContext()));

        if (node.scope() == SetStatement.Scope.LICENSE) {
            if (node.assignments().size() != AnalyzedSetLicenseStatement.LICENSE_TOKEN_NUM) {
                throw new IllegalArgumentException("Invalid number of arguments for SET LICENSE. " +
                                                   "Please provide only the license key");
            }
            return new AnalyzedSetLicenseStatement(statement.assignments().get(0).expression());
        }
        return new AnalyzedSetStatement(node.scope(), statement.assignments(), isPersistent);
    }
}
