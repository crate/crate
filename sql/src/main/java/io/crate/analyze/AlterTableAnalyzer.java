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
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.inject.Inject;

public class AlterTableAnalyzer extends AbstractStatementAnalyzer<Void, AlterTableAnalyzedStatement> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();
    private final ReferenceInfos referenceInfos;

    @Inject
    public AlterTableAnalyzer(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    @Override
    public Void visitColumnDefinition(ColumnDefinition node, AlterTableAnalyzedStatement context) {
        if (node.ident().startsWith("_")) {
            throw new IllegalArgumentException("Column ident must not start with '_'");
        }

        return null;
    }

    @Override
    public Void visitAlterTable(AlterTable node, AlterTableAnalyzedStatement context) {
        setTableAndPartitionName(node.table(), context);

        TableParameterInfo tableParameterInfo = context.table().tableParameterInfo();
        if (context.partitionName().isPresent()) {
            assert tableParameterInfo instanceof AlterPartitionedTableParameterInfo;
            tableParameterInfo = ((AlterPartitionedTableParameterInfo) tableParameterInfo).partitionTableSettingsInfo();
        }

        if (node.genericProperties().isPresent()) {
            TABLE_PROPERTIES_ANALYZER.analyze(
                    context.tableParameter(), tableParameterInfo, node.genericProperties(), context.parameters());
        } else if (!node.resetProperties().isEmpty()) {
            TABLE_PROPERTIES_ANALYZER.analyze(
                    context.tableParameter(), tableParameterInfo, node.resetProperties());
        }

        return null;
    }

    private void setTableAndPartitionName(Table node, AlterTableAnalyzedStatement context) {
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

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new AlterTableAnalyzedStatement(parameterContext, referenceInfos);
    }
}
