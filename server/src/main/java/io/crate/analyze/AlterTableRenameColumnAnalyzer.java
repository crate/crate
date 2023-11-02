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
import java.util.Objects;

import org.elasticsearch.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AlterTableRenameColumn;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;

public class AlterTableRenameColumnAnalyzer {
    private final Schemas schemas;
    private final NodeContext nodeCtx;

    public AlterTableRenameColumnAnalyzer(Schemas schemas, NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedAlterTableRenameColumn analyze(AlterTableRenameColumn<Expression> renameColumn,
                                                  ParamTypeHints paramTypeHints,
                                                  CoordinatorTxnCtx txnCtx) {
        if (!renameColumn.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Renaming a column from a single partition is not supported");
        }

        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            renameColumn.table().getName(),
            Operation.ALTER,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath());
        if (tableInfo.versionCreated().before(Version.V_5_5_0)) {
            throw new UnsupportedOperationException(
                "Renaming columns of a table created before version 5.5 is not supported"
            );
        }
        var expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            new FieldProviderResolvesUnknownColumns(tableInfo),
            null
        );
        var expressionContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        Reference sourceRef = (Reference) expressionAnalyzer.convert(renameColumn.column(), expressionContext);
        Reference targetRef = (Reference) expressionAnalyzer.convert(renameColumn.newName(), expressionContext);
        ColumnIdent sourceCol = sourceRef.column();
        ColumnIdent targetCol = targetRef.column();

        if (sourceCol.path().size() != targetCol.path().size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "Cannot rename a column to a name that has different column level: %s, %s",
                    sourceCol,
                    targetCol));
        }
        if (!Objects.equals(sourceCol.getParent(), targetCol.getParent())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "When renaming sub-columns, parent names must be equal: %s, %s",
                    sourceCol.getParent(),
                    targetCol.getParent()));
        }

        tableInfo.renameColumn(sourceRef, targetCol);
        return new AnalyzedAlterTableRenameColumn(tableInfo.ident(), sourceRef, targetCol);
    }

    /** Returns DynamicReferences instead of throwing ColumnUnknownExceptions. */
    public static class FieldProviderResolvesUnknownColumns implements FieldProvider<Reference> {

        private final DocTableInfo table;

        public FieldProviderResolvesUnknownColumns(DocTableInfo table) {
            this.table = table;
        }

        @Override
        @NotNull
        public Reference resolveField(QualifiedName qualifiedName,
                                      @Nullable List<String> path,
                                      Operation operation,
                                      boolean errorOnUnknownObjectKey) {
            var columnIdent = ColumnIdent.fromNameSafe(qualifiedName, path);
            var reference = table.getReference(columnIdent);
            if (reference == null) {
                reference = table.indexColumn(columnIdent);
                if (reference == null) {
                    return new DynamicReference(new ReferenceIdent(table.ident(), columnIdent), RowGranularity.DOC, -1);
                }
            }
            return reference;
        }
    }
}
