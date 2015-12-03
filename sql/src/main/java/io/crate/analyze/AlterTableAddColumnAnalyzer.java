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
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Table;
import io.crate.types.CollectionType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;

@Singleton
public class AlterTableAddColumnAnalyzer extends DefaultTraversalVisitor<AddColumnAnalyzedStatement, Analysis> {

    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final AnalysisMetaData analysisMetaData;

    @Inject
    public AlterTableAddColumnAnalyzer(Schemas schemas,
                                       FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                       AnalysisMetaData analysisMetaData) {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.analysisMetaData = analysisMetaData;
    }

    public AddColumnAnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return super.process(node, analysis);
    }

    @Override
    protected AddColumnAnalyzedStatement visitNode(Node node, Analysis analysis) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a AlterTableAddColumn node", node));
    }

    @Override
    public AddColumnAnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn node, Analysis analysis) {
        AddColumnAnalyzedStatement statement = new AddColumnAnalyzedStatement(schemas);
        setTableAndPartitionName(node.table(), statement, analysis.parameterContext());

        statement.analyzedTableElements(TableElementsAnalyzer.analyze(
                node.tableElement(),
                analysis.parameterContext(),
                fulltextAnalyzerResolver
        ));

        for (AnalyzedColumnDefinition column : statement.analyzedTableElements().columns()) {
            ensureColumnLeafsAreNew(column, statement.table());
        }
        addExistingPrimaryKeys(statement);
        ensureNoIndexDefinitions(statement.analyzedTableElements().columns());
        statement.analyzedTableElements().finalizeAndValidate(
                statement.table().ident(), analysisMetaData, analysis.parameterContext());

        int numCurrentPks = statement.table().primaryKey().size();
        if (statement.table().primaryKey().contains(new ColumnIdent("_id"))) {
            numCurrentPks -= 1;
        }
        statement.newPrimaryKeys(statement.analyzedTableElements().primaryKeys().size() > numCurrentPks);
        return statement;
    }

    private void ensureColumnLeafsAreNew(AnalyzedColumnDefinition column, TableInfo tableInfo) {
        if ((!column.isParentColumn() || !column.hasChildren()) && tableInfo.getReferenceInfo(column.ident()) != null) {
            throw new IllegalArgumentException(String.format(
                    "The table \"%s\" already has a column named \"%s\"",
                    tableInfo.ident().fqn(),
                    column.ident().sqlFqn()));
        }
        for (AnalyzedColumnDefinition child : column.children()) {
            ensureColumnLeafsAreNew(child, tableInfo);
        }
    }

    private void addExistingPrimaryKeys(AddColumnAnalyzedStatement context) {
        for (ColumnIdent pkIdent : context.table().primaryKey()) {
            if (pkIdent.name().equals("_id")) {
                continue;
            }
            ReferenceInfo pkInfo = context.table().getReferenceInfo(pkIdent);
            assert pkInfo != null;

            AnalyzedColumnDefinition pkColumn = new AnalyzedColumnDefinition(null);
            pkColumn.ident(pkIdent);
            pkColumn.name(pkIdent.name());
            pkColumn.isPrimaryKey(true);

            assert !(pkInfo.type() instanceof CollectionType); // pk can't be an array
            pkColumn.dataType(pkInfo.type().getName());
            context.analyzedTableElements().add(pkColumn);
        }

        for (ColumnIdent columnIdent : context.table().partitionedBy()) {
            context.analyzedTableElements().changeToPartitionedByColumn(columnIdent, true);
        }
    }

    private void ensureNoIndexDefinitions(List<AnalyzedColumnDefinition> columns) {
        for (AnalyzedColumnDefinition column : columns) {
            if (column.isIndex()) {
                throw new UnsupportedOperationException(
                        "Adding an index using ALTER TABLE ADD COLUMN is not supported");
            }
            ensureNoIndexDefinitions(column.children());
        }
    }

    private void setTableAndPartitionName(Table node, AddColumnAnalyzedStatement context, ParameterContext parameterContext) {
        if (!node.partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Adding a column to a single partition is not supported");
        }
        context.table(TableIdent.of(node, parameterContext.defaultSchema()));
    }

}
