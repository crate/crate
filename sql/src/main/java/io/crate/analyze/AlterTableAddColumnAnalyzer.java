/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.analyze.expressions.ExpressionToColumnIdentVisitor;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ParameterExpression;

import java.util.function.Function;

import static java.util.Collections.singletonList;

class AlterTableAddColumnAnalyzer {

    private final Schemas schemas;
    private final Functions functions;

    AlterTableAddColumnAnalyzer(Schemas schemas,
                                Functions functions) {
        this.schemas = schemas;
        this.functions = functions;
    }

    public AnalyzedAlterTableAddColumn analyze(AlterTableAddColumn<Expression> alterTable,
                                               SearchPath searchPath,
                                               Function<ParameterExpression, Symbol> convertParamFunction,
                                               CoordinatorTxnCtx txnCtx) {
        if (!alterTable.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Adding a column to a single partition is not supported");
        }
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(alterTable.table().getName(), Operation.ALTER,
                                                                         searchPath);
        TableReferenceResolver referenceResolver = new TableReferenceResolver(tableInfo.columns(), tableInfo.ident());

        var exprAnalyzerWithReferenceResolver = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, referenceResolver, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, FieldProvider.FIELDS_AS_LITERAL, null);
        var exprCtx = new ExpressionAnalysisContext();

        AddColumnDefinition<Expression> tableElement = alterTable.tableElement();
        // convert and validate the column name
        ExpressionToColumnIdentVisitor.convert(tableElement.name());

        AddColumnDefinition<Symbol> addColumnDefinition = tableElement.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx));
        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            singletonList(addColumnDefinition), tableInfo.ident(), tableInfo);

        // 2nd phase, analyze possible generated expressions
        AddColumnDefinition<Symbol> addColumnDefinitionWithExpression = (AddColumnDefinition<Symbol>) tableElement.mapExpressions(
            addColumnDefinition,
            x -> exprAnalyzerWithReferenceResolver.convert(x, exprCtx));
        AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions = TableElementsAnalyzer.analyze(
            singletonList(addColumnDefinitionWithExpression), tableInfo.ident(), tableInfo);


        return new AnalyzedAlterTableAddColumn(tableInfo, analyzedTableElements, analyzedTableElementsWithExpressions);
    }
}
