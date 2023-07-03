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

import java.util.List;
import java.util.Locale;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.relations.NewColumnFieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.AlterTableAddColumn;
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
        List<AddColumnDefinition<Expression>> tableElements = alterTable.tableElements();
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            alterTable.table().getName(),
            Operation.ALTER,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath());
        var tableReferenceResolver = new TableReferenceResolver(tableInfo.columns(), tableInfo.ident());
        SelfReferenceFieldProvider selfReferenceFieldProvider = new SelfReferenceFieldProvider(
            tableInfo.ident(),
            tableReferenceResolver,
            new NewColumnFieldProvider(tableInfo.ident(), tableElements)
        );

        var expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, selfReferenceFieldProvider, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        List<AddColumnDefinition<Symbol>> columnDefinitions = Lists2.map(
            tableElements,
            x -> x.map(expression -> expressionAnalyzer.convert(expression, exprCtx))
        );
        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            List.copyOf(columnDefinitions),
            tableInfo.ident(),
            tableInfo,
            true
        );

        return new AnalyzedAlterTableAddColumn(tableInfo, analyzedTableElements);
    }

    private static class SelfReferenceFieldProvider implements FieldProvider<Reference> {

        private final RelationName relationName;
        private final TableReferenceResolver referenceResolver;
        private final FieldProvider<Reference> unknownColumnFallback;

        SelfReferenceFieldProvider(RelationName relationName,
                                   TableReferenceResolver referenceResolver,
                                   FieldProvider<Reference> unknownColumnFallback) {
            this.relationName = relationName;
            this.referenceResolver = referenceResolver;
            this.unknownColumnFallback = unknownColumnFallback;
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
                Reference resolvedColumn = unknownColumnFallback.resolveField(qualifiedName, path, operation, errorOnUnknownObjectKey);
                if (resolvedColumn == null) {
                    ColumnIdent column = ColumnIdent.fromNameSafe(qualifiedName, path);
                    throw new ColumnUnknownException(column, relationName);
                }
                return resolvedColumn;
            }
        }
    }
}
