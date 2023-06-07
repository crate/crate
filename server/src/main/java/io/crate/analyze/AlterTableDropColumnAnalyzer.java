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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AlterTableDropColumn;
import io.crate.sql.tree.DropColumnDefinition;
import io.crate.sql.tree.Expression;

class AlterTableDropColumnAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    AlterTableDropColumnAnalyzer(Schemas schemas,
                                 NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedAlterTableDropColumn analyze(AlterTableDropColumn<Expression> alterTable,
                                                ParamTypeHints paramTypeHints,
                                                CoordinatorTxnCtx txnCtx) {
        if (!alterTable.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Dropping a column from a single partition is not supported");
        }

        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            alterTable.table().getName(),
            Operation.ALTER,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath());

        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        var exprCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        List<DropColumnDefinition<Expression>> tableElements = alterTable.tableElements();

        List<DropColumnDefinition<Symbol>> dropColumnDefinitions = tableElements.stream().map(
            tableElement -> new DropColumnDefinition<>(
                exprAnalyzerWithFieldsAsString.convert(tableElement.name(), exprCtx),
                tableElement.ifExists()
        )).toList();

        AnalyzedTableElements<Symbol> analyzedTableElements = TableElementsAnalyzer.analyze(
            List.copyOf(dropColumnDefinitions), tableInfo.ident(), tableInfo, false);

        dropColumnDefinitions = validate(tableInfo, analyzedTableElements, dropColumnDefinitions);
        analyzedTableElements = TableElementsAnalyzer.analyze(
            List.copyOf(dropColumnDefinitions), tableInfo.ident(), tableInfo, false);

        return new AnalyzedAlterTableDropColumn(tableInfo, analyzedTableElements);
    }

    /**
     * Validate that columns can be dropped and return and updated list of {@param dropColumnDefinitions},
     * where non-existent columns with `IF EXISTS` are removed, and therefore later on skipped. If all cols are defined
     * with `IF EXISTS` and none of them actually exist, then an empty list is returned.
     * {@link io.crate.planner.Planner} will then shortcut to a no-op.
     */
    private static List<DropColumnDefinition<Symbol>> validate(DocTableInfo tableInfo,
                                                               AnalyzedTableElements<Symbol> analyzedTableElements,
                                                               List<DropColumnDefinition<Symbol>> dropColumnDefinitions) {
        var generatedColRefs = new HashSet<>();
        for (var genRef : tableInfo.generatedColumns()) {
            generatedColRefs.addAll(genRef.referencedReferences());
        }
        List<DropColumnDefinition<Symbol>> updatedDropColDefs = new ArrayList<>(dropColumnDefinitions.size());

        for (int i = 0 ; i < analyzedTableElements.columns().size(); i++) {
            var colIdentToDrop = analyzedTableElements.columns().get(i).ident();
            var refToDrop = tableInfo.getReference(colIdentToDrop);
            var dropColDef = dropColumnDefinitions.get(i);
            if (refToDrop == null) {
                if (dropColDef.ifExists() == false) {
                    throw new ColumnUnknownException(colIdentToDrop, tableInfo.ident());
                } else {
                    continue;
                }
            }
            updatedDropColDefs.add(dropColDef);

            if (tableInfo.primaryKey().contains(refToDrop.column())) {
                throw new UnsupportedOperationException("Dropping column: " + colIdentToDrop.sqlFqn() + " which " +
                                                        "is part of the PRIMARY KEY is not allowed");
            }

            if (tableInfo.clusteredBy().equals(refToDrop.column())) {
                throw new UnsupportedOperationException("Dropping column: " + colIdentToDrop.sqlFqn() + " which " +
                                                        "is used in 'CLUSTERED BY' is not allowed");
            }

            if (tableInfo.isPartitioned() && tableInfo.partitionedBy().contains(colIdentToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colIdentToDrop.sqlFqn() + " which " +
                                                        "is part of the 'PARTITIONED BY' columns is not allowed");
            }

            if (generatedColRefs.contains(refToDrop)) {
                throw new UnsupportedOperationException(
                    "Dropping column: " + colIdentToDrop.sqlFqn() + " which is used to produce values for " +
                    "generated column is not allowed");
            }
        }

        var leftOverCols = tableInfo.columns().stream().map(Reference::column).collect(Collectors.toSet());
        leftOverCols.removeAll(analyzedTableElements.columnIdents());
        if (leftOverCols.isEmpty()) {
            throw new UnsupportedOperationException("Dropping all columns of a table is not allowed");
        }

        return updatedDropColDefs;
    }
}
