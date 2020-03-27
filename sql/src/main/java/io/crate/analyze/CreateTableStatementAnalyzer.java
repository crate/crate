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
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.EnsureNoMatchPredicate;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.TableElement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class CreateTableStatementAnalyzer {

    private final Functions functions;

    public CreateTableStatementAnalyzer(Functions functions) {
        this.functions = functions;
    }

    public AnalyzedCreateTable analyze(CreateTable<Expression> createTable,
                                       ParamTypeHints paramTypeHints,
                                       CoordinatorTxnCtx txnCtx) {
        RelationName relationName = RelationName
            .of(createTable.name().getName(), txnCtx.sessionContext().searchPath().currentSchema());
        relationName.ensureValidForRelationCreation();

        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            functions, txnCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            functions, txnCtx, paramTypeHints, FieldProvider.FIELDS_AS_LITERAL, null);
        var exprCtx = new ExpressionAnalysisContext();

        // 1st phase, map and analyze everything BESIDES [check constraint, generated, default] expressions
        CreateTable<Symbol> analyzedCreateTable = new CreateTable<>(
            createTable.name().map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx)),
            analyzeButCheckConstraints(createTable, exprAnalyzerWithFieldsAsString, exprCtx),
            createTable.partitionedBy().map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.clusteredBy().map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx))),
            createTable.properties().map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx)),
            createTable.ifNotExists()
        );
        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            analyzedCreateTable.tableElements(), relationName, null);

        // 2nd phase, analyze generated/default Expressions and check constraints with a reference resolver
        TableReferenceResolver referenceResolver = analyzedTableElements.referenceResolver(relationName);
        var exprAnalyzerWithReferences = new ExpressionAnalyzer(
            functions, txnCtx, paramTypeHints, referenceResolver, null);
        List<TableElement<Symbol>> tableElementsWithExpressions = new ArrayList<>();
        for (int i = 0; i < analyzedCreateTable.tableElements().size(); i++) {
            TableElement<Expression> elementExpression = createTable.tableElements().get(i);
            TableElement<Symbol> elementSymbol = analyzedCreateTable.tableElements().get(i);
            tableElementsWithExpressions.add(elementExpression.mapExpressions(elementSymbol, x -> {
                Symbol symbol = exprAnalyzerWithReferences.convert(x, exprCtx);
                EnsureNoMatchPredicate.ensureNoMatchPredicate(symbol, "Cannot use MATCH in CREATE TABLE statements");
                return symbol;
            }));
        }
        List<TableElement<Symbol>> analyzedCheckConstraints = createTable.tableElements()
            .stream()
            .filter(x -> x instanceof CheckConstraint)
            .map(x -> (CheckConstraint<Expression>) x)
            .map(x -> x.map(y -> exprAnalyzerWithReferences.convert(y, exprCtx)))
            .collect(Collectors.toList());
        tableElementsWithExpressions.addAll(analyzedCheckConstraints);
        analyzedCreateTable.tableElements().addAll(analyzedCheckConstraints);
        analyzedCheckConstraints.forEach(c -> analyzedTableElements.addCheckConstraint(relationName, (CheckConstraint<Symbol>) c));
        AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions = TableElementsAnalyzer.analyze(
            tableElementsWithExpressions, relationName, null, false);
        return new AnalyzedCreateTable(
            relationName,
            analyzedCreateTable,
            analyzedTableElements,
            analyzedTableElementsWithExpressions
        );
    }

    private static List<TableElement<Symbol>> analyzeButCheckConstraints(CreateTable<Expression> createTable,
                                                                         ExpressionAnalyzer exprAnalyzerWithFieldsAsString,
                                                                         ExpressionAnalysisContext exprCtx) {
        List<TableElement<Expression>> notCheckConstraints = createTable.tableElements()
            .stream()
            .filter(x -> false == x instanceof CheckConstraint)
            .collect(Collectors.toList());
        List<TableElement<Symbol>> analyzed = new ArrayList<>(notCheckConstraints.size());
        Set<CheckColumnConstraint<Expression>> checkColumnConstraints = new HashSet<>(notCheckConstraints.size());
        for (int i = 0; i < notCheckConstraints.size(); i++) {
            TableElement<Expression> te = notCheckConstraints.get(i);
            if (te instanceof ColumnDefinition) {
                ColumnDefinition<Expression> def = (ColumnDefinition<Expression>) te;
                List<ColumnConstraint<Expression>> constraints = def.constraints();
                for (int j = 0; j < constraints.size(); j++) {
                    ColumnConstraint<Expression> cc = constraints.get(j);
                    if (cc instanceof CheckColumnConstraint) {
                        CheckColumnConstraint<Expression> check = (CheckColumnConstraint<Expression>) cc;
                        checkColumnConstraints.add(check);
                        // Re-frame the column constraint as a table constraint
                        createTable.tableElements().add(new CheckConstraint<>(
                            check.name(),
                            def.ident(),
                            check.expression(),
                            check.expressionStr()
                        ));
                    }
                }
                def.constraints().removeAll(checkColumnConstraints);
            }
            analyzed.add(te.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx)));
        }
        return analyzed;
    }
}
