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

import io.crate.PartitionName;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Table;
import io.crate.types.CollectionType;
import org.elasticsearch.common.inject.Inject;

import java.util.Collection;

public class AlterTableAddColumnAnalyzer extends AbstractStatementAnalyzer<Void, AddColumnAnalysis> {

    private final ReferenceInfos referenceInfos;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    @Inject
    public AlterTableAddColumnAnalyzer(ReferenceInfos referenceInfos,
                                       FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.referenceInfos = referenceInfos;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    @Override
    public Analysis newAnalysis(Analyzer.ParameterContext parameterContext) {
        return new AddColumnAnalysis(referenceInfos, fulltextAnalyzerResolver, parameterContext);
    }

    @Override
    protected Void visitNode(Node node, AddColumnAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a AlterTableAddColumn node", node));
    }

    @Override
    public Void visitAlterTableAddColumnStatement(AlterTableAddColumn node, AddColumnAnalysis context) {
        setTableAndPartitionName(node.table(), context);

        context.analyzedTableElements(TableElementsAnalyzer.analyze(
                node.tableElement(),
                context.parameters(),
                context.fulltextAnalyzerResolver()
        ));

        for (AnalyzedColumnDefinition column : context.analyzedTableElements().columns()) {
            ensureColumnLeafsAreNew(column, context.table());
        }
        addExistingPrimaryKeys(context);
        ensureNoIndexDefinitions(context.analyzedTableElements().columns());
        context.analyzedTableElements().finalizeAndValidate();

        int numCurrentPks = context.table().primaryKey().size();
        if (context.table().primaryKey().contains(new ColumnIdent("_id"))) {
            numCurrentPks -= 1;
        }
        context.newPrimaryKeys(context.analyzedTableElements().primaryKeys().size() > numCurrentPks);
        return null;
    }

    private void ensureColumnLeafsAreNew(AnalyzedColumnDefinition column, TableInfo tableInfo) {
        if ((!column.isObjectExtension() || column.children().isEmpty())
                && tableInfo.getReferenceInfo(column.ident()) != null) {
            throw new IllegalArgumentException(String.format(
                    "The table \"%s\" already has a column named \"%s\"",
                    tableInfo.ident().name(),
                    column.ident().sqlFqn()));
        }
        for (AnalyzedColumnDefinition child : column.children()) {
            ensureColumnLeafsAreNew(child, tableInfo);
        }
    }

    private void addExistingPrimaryKeys(AddColumnAnalysis context) {
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
            context.analyzedTableElements().changeToPartitionedByColumn(columnIdent);
        }
    }

    private void ensureNoIndexDefinitions(Collection<AnalyzedColumnDefinition> columns) {
        for (AnalyzedColumnDefinition column : columns) {
            if (column.isIndex()) {
                throw new UnsupportedOperationException(
                        "Adding an index using ALTER TABLE ADD COLUMN is not supported");
            }
            ensureNoIndexDefinitions(column.children());
        }
    }

    private void setTableAndPartitionName(Table node, AddColumnAnalysis context) {
        context.table(TableIdent.of(node));
        if (!node.partitionProperties().isEmpty()) {
            PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                    context.table(),
                    node.partitionProperties(),
                    context.parameters());
            if (!context.table().partitions().contains(partitionName)) {
                throw new IllegalArgumentException("Referenced partition does not exist.");
            }
            context.partitionName(partitionName);
        }
    }
}
