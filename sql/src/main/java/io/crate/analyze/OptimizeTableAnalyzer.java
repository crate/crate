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
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.Table;

import java.util.HashMap;
import java.util.function.Function;

public class OptimizeTableAnalyzer {

    private final Schemas schemas;
    private final Functions functions;

    OptimizeTableAnalyzer(Schemas schemas, Functions functions) {
        this.schemas = schemas;
        this.functions = functions;
    }

    public AnalyzedOptimizeTable analyze(OptimizeStatement<Expression> statement,
                                         Function<ParameterExpression, Symbol> convertParamFunction,
                                         CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, FieldProvider.FIELDS_AS_LITERAL, null);

        var exprCtx = new ExpressionAnalysisContext();
        OptimizeStatement<Symbol> analyzedStatement =
            statement.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx));

        HashMap<Table<Symbol>, TableInfo> analyzedOptimizeTables = new HashMap<>();
        for (Table<Symbol> table : analyzedStatement.tables()) {
            TableInfo tableInfo = schemas.resolveTableInfo(
                table.getName(),
                Operation.OPTIMIZE,
                txnCtx.sessionContext().user(),
                txnCtx.sessionContext().searchPath()
            );
            analyzedOptimizeTables.put(table, tableInfo);
        }
        return new AnalyzedOptimizeTable(analyzedOptimizeTables, analyzedStatement.properties());
    }
}
