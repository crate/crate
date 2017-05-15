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

import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.types.CollectionType;

import java.util.List;
import java.util.Locale;

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
        TableIdent tableIdent = TableIdent.of(node.table(), analysis.sessionContext().defaultSchema());
        DocTableInfo tableInfo = schemas.getTableInfo(tableIdent, Operation.ALTER, analysis.sessionContext().user());
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
            analysis.sessionContext());

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
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            if (pkIdent.name().equals("_id")) {
                continue;
            }
            Reference pkInfo = tableInfo.getReference(pkIdent);
            assert pkInfo != null : "pk must not be null";

            AnalyzedColumnDefinition pkColumn = new AnalyzedColumnDefinition(null);
            pkColumn.ident(pkIdent);
            pkColumn.name(pkIdent.name());
            pkColumn.setPrimaryKeyConstraint();

            assert !(pkInfo.valueType() instanceof CollectionType) : "pk can't be an array";
            pkColumn.dataType(pkInfo.valueType().getName());
            tableElements.add(pkColumn);
        }

        for (ColumnIdent columnIdent : tableInfo.partitionedBy()) {
            tableElements.changeToPartitionedByColumn(columnIdent, true);
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
