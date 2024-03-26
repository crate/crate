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

import java.util.HashMap;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.Table;

class RefreshTableAnalyzer {

    private final NodeContext nodeCtx;
    private final Schemas schemas;

    RefreshTableAnalyzer(NodeContext nodeCtx, Schemas schemas) {
        this.nodeCtx = nodeCtx;
        this.schemas = schemas;
    }

    public AnalyzedRefreshTable analyze(RefreshStatement<Expression> refreshStatement,
                                        ParamTypeHints paramTypeHints,
                                        CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        HashMap<Table<Symbol>, DocTableInfo> analyzedTables = new HashMap<>();
        for (var table : refreshStatement.tables()) {
            var analyzedTable = table.map(t -> exprAnalyzerWithFieldsAsString.convert(t, exprCtx));
            DocTableInfo tableInfo = schemas.findRelation(
                table.getName(),
                Operation.REFRESH,
                txnCtx.sessionSettings().sessionUser(),
                txnCtx.sessionSettings().searchPath()
            );
            analyzedTables.put(analyzedTable, tableInfo);
        }
        return new AnalyzedRefreshTable(analyzedTables);
    }
}
