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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AlterTableAddColumn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

class AlterTableAddColumnAnalyzer {

    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final Functions functions;

    AlterTableAddColumnAnalyzer(Schemas schemas,
                                FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                Functions functions) {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.functions = functions;
    }

    public AddColumnAnalyzedStatement analyze(AlterTableAddColumn node, Analysis analysis) {
        if (!node.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Adding a column to a single partition is not supported");
        }
        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(node.table().getName(), Operation.ALTER,
            analysis.sessionContext().searchPath());
        AnalyzedTableElements tableElements = TableElementsAnalyzer.analyze(
            node.tableElement(),
            analysis.parameterContext().parameters(),
            fulltextAnalyzerResolver,
            tableInfo
        );
        for (AnalyzedColumnDefinition column : tableElements.columns()) {
            ensureColumnLeafsAreNew(column, tableInfo);
        }
        addExistingPrimaryKeys(tableInfo, tableElements);
        ensureNoIndexDefinitions(tableElements.columns());
        tableElements.finalizeAndValidate(
            tableInfo.ident(),
            tableInfo.columns(),
            functions,
            analysis.parameterContext(),
            analysis.transactionContext());

        int numCurrentPks = tableInfo.primaryKey().size();
        if (tableInfo.primaryKey().contains(DocSysColumns.ID)) {
            numCurrentPks -= 1;
        }

        boolean hasNewPrimaryKeys = tableElements.primaryKeys().size() > numCurrentPks;
        boolean hasGeneratedColumns = tableElements.hasGeneratedColumns();
        return new AddColumnAnalyzedStatement(
            tableInfo,
            tableElements,
            hasNewPrimaryKeys,
            hasGeneratedColumns
        );
    }

    private static void ensureColumnLeafsAreNew(AnalyzedColumnDefinition column, TableInfo tableInfo) {
        if ((!column.isParentColumn() || !column.hasChildren()) && tableInfo.getReference(column.ident()) != null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "The table %s already has a column named %s",
                tableInfo.ident().sqlFqn(),
                column.ident().sqlFqn()));
        }
        for (AnalyzedColumnDefinition child : column.children()) {
            ensureColumnLeafsAreNew(child, tableInfo);
        }
    }

    private static void addExistingPrimaryKeys(DocTableInfo tableInfo, AnalyzedTableElements tableElements) {
        LinkedHashSet<ColumnIdent> pkIncludingAncestors = new LinkedHashSet<>();
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            if (pkIdent.name().equals(DocSysColumns.Names.ID)) {
                continue;
            }
            ColumnIdent maybeParent = pkIdent;
            pkIncludingAncestors.add(maybeParent);
            while ((maybeParent = maybeParent.getParent()) != null) {
                pkIncludingAncestors.add(maybeParent);
            }
        }
        ArrayList<ColumnIdent> columnsToBuildHierarchy = new ArrayList<>(pkIncludingAncestors);
        // We want to have the root columns earlier in the list so that the loop below can be sure parent elements are already present in `columns`
        columnsToBuildHierarchy.sort(Comparator.comparingInt(c -> c.path().size()));
        HashMap<ColumnIdent, AnalyzedColumnDefinition> columns = new HashMap<>();
        for (ColumnIdent column : columnsToBuildHierarchy) {
            ColumnIdent parent = column.getParent();
            // sort of `columnsToBuildHierarchy` ensures parent would already have been processed and must be present in columns
            AnalyzedColumnDefinition parentDef = columns.get(parent);
            AnalyzedColumnDefinition columnDef = new AnalyzedColumnDefinition(null, parentDef);
            columns.put(column, columnDef);
            columnDef.ident(column);
            if (tableInfo.primaryKey().contains(column)) {
                columnDef.setPrimaryKeyConstraint();
            }
            Reference reference = Objects.requireNonNull(
                tableInfo.getReference(column),
                "Must be able to retrieve Reference for any column that is part of `primaryKey()`");
            columnDef.dataType(reference.valueType().getName());
            if (parentDef != null) {
                parentDef.addChild(columnDef);
            }
            if (column.isTopLevel()) {
                tableElements.add(columnDef);
            }
        }
        for (ColumnIdent columnIdent : tableInfo.partitionedBy()) {
            tableElements.changeToPartitionedByColumn(columnIdent, true, tableInfo.ident());
        }
    }

    private static void ensureNoIndexDefinitions(List<AnalyzedColumnDefinition> columns) {
        for (AnalyzedColumnDefinition column : columns) {
            if (column.isIndexColumn()) {
                throw new UnsupportedOperationException(
                    "Adding an index using ALTER TABLE ADD COLUMN is not supported");
            }
            ensureNoIndexDefinitions(column.children());
        }
    }
}
