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

import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

@Singleton
public class AlterTableAnalyzer extends DefaultTraversalVisitor<AlterTableAnalyzedStatement, Analysis> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();
    private final Schemas schemas;

    @Inject
    public AlterTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    @Override
    public AlterTableAnalyzedStatement visitColumnDefinition(ColumnDefinition node, Analysis analysis) {
        if (node.ident().startsWith("_")) {
            throw new IllegalArgumentException("Column ident must not start with '_'");
        }

        return null;
    }

    @Override
    public AlterTableAnalyzedStatement visitAlterTable(AlterTable node, Analysis analysis) {
        AlterTableAnalyzedStatement statement = new AlterTableAnalyzedStatement(schemas);
        setTableAndPartitionName(node.table(), statement, analysis);

        TableParameterInfo tableParameterInfo = statement.table().tableParameterInfo();
        if (statement.partitionName().isPresent()) {
            assert tableParameterInfo instanceof AlterPartitionedTableParameterInfo;
            assert !node.table().excludePartitions() : "Alter table ONLY not supported when using a partition";
            tableParameterInfo = ((AlterPartitionedTableParameterInfo) tableParameterInfo).partitionTableSettingsInfo();
        }
        statement.excludePartitions(node.table().excludePartitions());

        if (node.genericProperties().isPresent()) {
            TABLE_PROPERTIES_ANALYZER.analyze(
                statement.tableParameter(), tableParameterInfo, node.genericProperties(),
                analysis.parameterContext().parameters());
        } else if (!node.resetProperties().isEmpty()) {
            TABLE_PROPERTIES_ANALYZER.analyze(
                statement.tableParameter(), tableParameterInfo, node.resetProperties());
        }

        // Only check for permission if statement is not changing the metadata blocks, so don't block `re-enabling` these.
        Settings tableSettings = statement.tableParameter().settings();
        if (tableSettings.getAsMap().size() != 1 ||
            (tableSettings.get(IndexMetaData.SETTING_BLOCKS_METADATA) == null &&
             tableSettings.get(IndexMetaData.SETTING_READ_ONLY) == null)) {

            Operation.blockedRaiseException(statement.table(), Operation.ALTER);
        }

        return statement;
    }

    private void setTableAndPartitionName(Table node, AlterTableAnalyzedStatement context, Analysis analysis) {
        context.table(TableIdent.of(node, analysis.parameterContext().defaultSchema()));
        if (!node.partitionProperties().isEmpty()) {
            PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                context.table(),
                node.partitionProperties(),
                analysis.parameterContext().parameters());
            if (!context.table().partitions().contains(partitionName)) {
                throw new IllegalArgumentException("Referenced partition does not exist.");
            }
            context.partitionName(partitionName);
        }
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        return process(node, analysis);
    }
}
