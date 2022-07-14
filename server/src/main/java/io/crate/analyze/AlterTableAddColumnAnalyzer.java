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

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;

class AlterTableAddColumnAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    AlterTableAddColumnAnalyzer(Schemas schemas,
                                NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedAlterTableAddColumn analyze(AlterTableAddColumn<Expression> alterTable,
                                               ParamTypeHints paramTypeHints,
                                               CoordinatorTxnCtx txnCtx) {
        if (!alterTable.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Adding a column to a single partition is not supported");
        }
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            alterTable.table().getName(),
            Operation.ALTER,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath());
        TableReferenceResolver referenceResolver = new TableReferenceResolver(tableInfo.columns(), tableInfo.ident());

        var exprAnalyzerWithReferenceResolver = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, referenceResolver, null);
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        AddColumnDefinition<Expression> tableElement = alterTable.tableElement();

        // 1st phase, exclude check constraints (their expressions contain column references) and generated expressions
        AddColumnDefinition<Symbol> addColumnDefinition = new AddColumnDefinition<>(
            exprAnalyzerWithFieldsAsString.convert(tableElement.name(), exprCtx),
            null,   // expression must be mapped later on using mapExpressions()
            tableElement.type() == null ? null : tableElement.type().map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx)),
            tableElement.constraints()
                .stream()
                .filter(c -> false == c instanceof CheckColumnConstraint)
                .map(x -> x.map(y -> exprAnalyzerWithFieldsAsString.convert(y, exprCtx)))
                .collect(Collectors.toList()),
            false,
            tableElement.generatedExpression() != null
        );
        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            singletonList(addColumnDefinition), tableInfo.ident(), tableInfo, true);

        // 2nd phase, analyze possible generated expressions
        AddColumnDefinition<Symbol> addColumnDefinitionWithExpression = (AddColumnDefinition<Symbol>) tableElement.mapExpressions(
            addColumnDefinition,
            x -> exprAnalyzerWithReferenceResolver.convert(x, exprCtx));
        AnalyzedTableElements<Symbol> analyzedTableElementsWithExpressions = TableElementsAnalyzer.analyze(
            singletonList(addColumnDefinitionWithExpression), tableInfo.ident(), tableInfo, true);
        // now analyze possible check expressions
        var checkColumnConstraintsAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            new SelfReferenceFieldProvider(
                tableInfo.ident(), referenceResolver, analyzedTableElements.columns()),
            null);
        tableElement.constraints()
            .stream()
            .filter(CheckColumnConstraint.class::isInstance)
            .map(x -> x.map(y -> checkColumnConstraintsAnalyzer.convert(y, exprCtx)))
            .forEach(c -> {
                CheckColumnConstraint<Symbol> check = (CheckColumnConstraint<Symbol>) c;
                analyzedTableElements.addCheckColumnConstraint(tableInfo.ident(), check);
                analyzedTableElementsWithExpressions.addCheckColumnConstraint(tableInfo.ident(), check);
            });
        if (addColumnDefinitionWithExpression.generatedExpression() != null) {
            GeneratedColumnValidator.validate(
                addColumnDefinitionWithExpression.generatedExpression(),
                tableInfo.ident(),
                analyzedTableElements.columnIdents().iterator().next().name(),
                tableInfo.generatedColumns().stream().map(GeneratedReference::column).toList());
        }
        return new AnalyzedAlterTableAddColumn(tableInfo, analyzedTableElements, analyzedTableElementsWithExpressions);
    }

    private static class SelfReferenceFieldProvider implements FieldProvider<Reference> {

        private final RelationName relationName;
        private final TableReferenceResolver referenceResolver;
        private final List<AnalyzedColumnDefinition<Symbol>> columnDefinitions;

        SelfReferenceFieldProvider(RelationName relationName,
                                   TableReferenceResolver referenceResolver,
                                   List<AnalyzedColumnDefinition<Symbol>> columnDefinitions) {
            this.relationName = relationName;
            this.referenceResolver = referenceResolver;
            this.columnDefinitions = columnDefinitions;
        }

        @Override
        public Reference resolveField(QualifiedName qualifiedName,
                                      @Nullable List<String> path,
                                      Operation operation,
                                      boolean errorOnUnknownObjectKey) {
            try {
                // SQL Semantics: CHECK expressions cannot refer to other
                // columns to not invalidate existing data inadvertently.
                Reference ref = referenceResolver.resolveField(qualifiedName, path, operation, errorOnUnknownObjectKey);
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "CHECK expressions defined in this context cannot refer to other columns: %s",
                    ref));
            } catch (ColumnUnknownException cue) {
                ColumnIdent colIdent = ColumnIdent.fromNameSafe(qualifiedName, path);
                for (int i = 0; i < columnDefinitions.size(); i++) {
                    AnalyzedColumnDefinition<Symbol> def = columnDefinitions.get(i);
                    if (def.ident().equals(colIdent)) {
                        return new SimpleReference(
                            new ReferenceIdent(relationName, colIdent),
                            RowGranularity.DOC,
                            def.dataType(),
                            def.position,
                            def.defaultExpression()
                        );
                    }
                }
                throw new ColumnUnknownException(colIdent.sqlFqn(), relationName);
            }
        }
    }
}
