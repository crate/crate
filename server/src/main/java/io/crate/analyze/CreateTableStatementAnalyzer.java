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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.EnsureNoMatchPredicate;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.TableElement;

public final class CreateTableStatementAnalyzer {

    private final NodeContext nodeCtx;

    public CreateTableStatementAnalyzer(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedCreateTable analyze(CreateTable<Expression> createTable,
                                       ParamTypeHints paramTypeHints,
                                       CoordinatorTxnCtx txnCtx) {
        RelationName relationName = RelationName
            .of(createTable.name().getName(), txnCtx.sessionSettings().searchPath().currentSchema());
        relationName.ensureValidForRelationCreation();

        var exprAnalyzerWithoutFields = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        Function<Expression, Symbol> exprMapper = y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx);

        // 1st phase, map and analyze everything EXCEPT:
        //   - check constraints defined at any level (table or column)
        //   - generated expressions
        //   - default expressions
        Map<TableElement<Symbol>, TableElement<Expression>> analyzed = new LinkedHashMap<>();
        List<CheckConstraint<Expression>> checkConstraints = new ArrayList<>();
        for (int i = 0; i < createTable.tableElements().size(); i++) {
            TableElement<Expression> te = createTable.tableElements().get(i);
            if (te instanceof CheckConstraint) {
                checkConstraints.add((CheckConstraint<Expression>) te);
                continue;
            }
            TableElement<Symbol> analyzedTe = null;
            if (te instanceof ColumnDefinition) {
                ColumnDefinition<Expression> def = (ColumnDefinition<Expression>) te;
                List<ColumnConstraint<Symbol>> analyzedColumnConstraints = new ArrayList<>();
                for (int j = 0; j < def.constraints().size(); j++) {
                    ColumnConstraint<Expression> cc = def.constraints().get(j);
                    if (cc instanceof CheckColumnConstraint) {
                        // Re-frame the column check constraint as a table check constraint
                        CheckColumnConstraint<Expression> columnCheck = (CheckColumnConstraint<Expression>) cc;
                        checkConstraints.add(new CheckConstraint<>(
                            columnCheck.name(),
                            def.ident(),
                            columnCheck.expression(),
                            columnCheck.expressionStr()
                        ));
                        continue;
                    }
                    analyzedColumnConstraints.add(cc.map(exprMapper));
                }
                analyzedTe = new ColumnDefinition<>(
                    def.ident(),
                    null,
                    null,
                    def.type() == null ? null : def.type().map(exprMapper),
                    analyzedColumnConstraints,
                    false,
                    def.isGenerated());
            }
            analyzed.put(analyzedTe == null ? te.map(exprMapper) : analyzedTe, te);
        }
        CreateTable<Symbol> analyzedCreateTable = new CreateTable<>(
            createTable.name().map(exprMapper),
            new ArrayList<>(analyzed.keySet()),
            createTable.partitionedBy().map(x -> x.map(exprMapper)),
            createTable.clusteredBy().map(x -> x.map(exprMapper)),
            createTable.properties().map(x -> exprAnalyzerWithoutFields.convert(x, exprCtx)),
            createTable.ifNotExists()
        );
        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            analyzedCreateTable.tableElements(), relationName, null, false);

        // 2nd phase, analyze and map with a reference resolver:
        //   - generated/default expressions
        //   - check constraints
        TableReferenceResolver referenceResolver = analyzedTableElements.referenceResolver(relationName);
        var exprAnalyzerWithReferences = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, referenceResolver, null);
        List<TableElement<Symbol>> tableElementsWithExpressions = new ArrayList<>();
        for (int i = 0; i < analyzedCreateTable.tableElements().size(); i++) {
            TableElement<Symbol> elementSymbol = analyzedCreateTable.tableElements().get(i);
            TableElement<Expression> elementExpression = analyzed.get(elementSymbol);
            tableElementsWithExpressions.add(elementExpression.mapExpressions(elementSymbol, x -> {
                Symbol symbol = exprAnalyzerWithReferences.convert(x, exprCtx);
                EnsureNoMatchPredicate.ensureNoMatchPredicate(symbol, "Cannot use MATCH in CREATE TABLE statements");
                return symbol;
            }));
        }
        checkConstraints
            .stream()
            .map(x -> x.map(y -> exprAnalyzerWithReferences.convert(y, exprCtx)))
            .forEach(te -> {
                analyzedCreateTable.tableElements().add(te);
                tableElementsWithExpressions.add(te);
                analyzedTableElements.addCheckConstraint(relationName, (CheckConstraint<Symbol>) te);
            });
        AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions = TableElementsAnalyzer.analyze(
            tableElementsWithExpressions, relationName, null, false, false);
        if (analyzedTableElementsWithExpressions.hasGeneratedColumns()) {
            List<ColumnIdent> generatedColumns = analyzedTableElementsWithExpressions.columns().stream()
                .filter(AnalyzedColumnDefinition::isGenerated)
                .map(AnalyzedColumnDefinition::ident).toList();
            analyzedTableElementsWithExpressions.columns().stream().filter(AnalyzedColumnDefinition::isGenerated).forEach(
                s -> GeneratedColumnValidator.validate(s.generatedExpression(), relationName, s.name(), generatedColumns)
            );
        }
        return new AnalyzedCreateTable(
            relationName,
            analyzedCreateTable,
            analyzedTableElements,
            analyzedTableElementsWithExpressions
        );
    }
}
