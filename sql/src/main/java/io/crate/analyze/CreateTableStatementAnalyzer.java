/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.TableElement;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class CreateTableStatementAnalyzer {

    private final Functions functions;

    public CreateTableStatementAnalyzer(Functions functions) {
        this.functions = functions;
    }

    public AnalyzedCreateTable analyze(CreateTable<Expression> createTable,
                                       Function<ParameterExpression, Symbol> convertParamFunction,
                                       CoordinatorTxnCtx txnCtx) {
        RelationName relationName = RelationName
            .of(createTable.name().getName(), txnCtx.sessionContext().searchPath().currentSchema());
        relationName.ensureValidForRelationCreation();

        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, FieldProvider.FIELDS_AS_LITERAL, null);
        var exprCtx = new ExpressionAnalysisContext();

        // 1st phase, map and analyze everything BESIDE generated and default expressions and
        CreateTable<Symbol> analyzedCreateTable = new CreateTable<>(
            createTable.name().map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx)),
            Lists2.map(createTable.tableElements(), x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.partitionedBy().map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.clusteredBy().map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.properties().map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx)),
            createTable.ifNotExists()
        );
        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            analyzedCreateTable.tableElements(), relationName, null);

        // 2nd phase, analyze possible generatedExpressions and defaultExpressions with a reference resolver
        TableReferenceResolver referenceResolver = analyzedTableElements.referenceResolver(relationName);
        var exprAnalyzerWithReferences = new ExpressionAnalyzer(
            functions, txnCtx, convertParamFunction, referenceResolver, null);
        List<TableElement<Symbol>> tableElementsWithExpressions = new ArrayList<>(analyzedCreateTable.tableElements().size());
        for (int i = 0; i < analyzedCreateTable.tableElements().size(); i++) {
            TableElement<Expression> elementExpression = createTable.tableElements().get(i);
            TableElement<Symbol> elementSymbol = analyzedCreateTable.tableElements().get(i);
            tableElementsWithExpressions.add(elementExpression.mapExpressions(elementSymbol, x -> exprAnalyzerWithReferences.convert(x, exprCtx)));
        }
        AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions = TableElementsAnalyzer.analyze(
            tableElementsWithExpressions, relationName, null, false);

        return new AnalyzedCreateTable(relationName, analyzedCreateTable, analyzedTableElements, analyzedTableElementsWithExpressions);
    }
}
