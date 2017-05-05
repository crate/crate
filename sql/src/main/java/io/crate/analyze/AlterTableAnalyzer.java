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

import io.crate.action.sql.SessionContext;
import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Table;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;

class AlterTableAnalyzer {

    private final Schemas schemas;

    AlterTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AlterTableAnalyzedStatement analyze(AlterTable node, Row parameters, SessionContext sessionContext) {
        Table table = node.table();
        DocTableInfo docTableInfo = schemas.getTableInfo(TableIdent.of(table, sessionContext.defaultSchema()),
            Operation.ALTER_BLOCKS, sessionContext.user());
        PartitionName partitionName = createPartitionName(table.partitionProperties(), docTableInfo, parameters);
        TableParameterInfo tableParameterInfo = getTableParameterInfo(table, docTableInfo, partitionName);
        TableParameter tableParameter = getTableParameter(node, parameters, tableParameterInfo);
        maybeRaiseBlockedException(docTableInfo, tableParameter.settings());
        return new AlterTableAnalyzedStatement(docTableInfo, partitionName, tableParameter, table.excludePartitions());
    }

    AlterTableRenameAnalyzedStatement analyzeRename(AlterTableRename node, String defaultSchema,
                                                    SessionContext sessionContext) {
        TableIdent tableIdent = TableIdent.of(node.table(), defaultSchema);
        DocTableInfo tableInfo = schemas.getTableInfo(tableIdent, sessionContext.user());
        if (!node.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Renaming a single partition is not supported");
        }

        List<String> newIdentParts = node.newName().getParts();
        if (newIdentParts.size() > 1) {
            throw new IllegalArgumentException("Target table name must not include a schema");
        }

        TableIdent newTableIdent = new TableIdent(tableIdent.schema(), newIdentParts.get(0));
        return new AlterTableRenameAnalyzedStatement(tableInfo, newTableIdent);
    }

    private static TableParameterInfo getTableParameterInfo(Table table,
                                                            DocTableInfo tableInfo,
                                                            @Nullable PartitionName partitionName) {
        TableParameterInfo tableParameterInfo = tableInfo.tableParameterInfo();
        if (partitionName == null) {
            return tableParameterInfo;
        }
        assert tableParameterInfo instanceof PartitionedTableParameterInfo :
            "tableParameterInfo must be " + PartitionedTableParameterInfo.class.getSimpleName();
        assert !table.excludePartitions() : "Alter table ONLY not supported when using a partition";
        return ((PartitionedTableParameterInfo) tableParameterInfo).partitionTableSettingsInfo();
    }

    private static TableParameter getTableParameter(AlterTable node, Row parameters, TableParameterInfo tableParameterInfo) {
        TableParameter tableParameter = new TableParameter();
        if (node.genericProperties().isPresent()) {
            TablePropertiesAnalyzer.analyze(tableParameter, tableParameterInfo, node.genericProperties(), parameters);
        } else if (!node.resetProperties().isEmpty()) {
            TablePropertiesAnalyzer.analyze(tableParameter, tableParameterInfo, node.resetProperties());
        }
        return tableParameter;
    }

    // Only check for permission if statement is not changing the metadata blocks, so don't block `re-enabling` these.
    private static void maybeRaiseBlockedException(DocTableInfo tableInfo, Settings tableSettings) {
        if (tableSettings.getAsMap().size() != 1 ||
            (tableSettings.get(IndexMetaData.SETTING_BLOCKS_METADATA) == null &&
             tableSettings.get(IndexMetaData.SETTING_READ_ONLY) == null)) {

            Operation.blockedRaiseException(tableInfo, Operation.ALTER);
        }
    }

    /**
     * Creates and returns a PartitionName based on a list of partition properties, table info and row params from
     * the query. Used so that the analyzer/operation can determine the partition supplied with the query.
     *
     * @param partitionsProperties A list of partition property assignments
     * @param tableInfo The table info of the relevant table
     * @param parameters The parameters supplied with the query
     * @return An instance of PartitionName based on the supplied partition properties, table info and params.
     */
    @Nullable
    public static PartitionName createPartitionName(List<Assignment> partitionsProperties,
                                                    DocTableInfo tableInfo,
                                                    Row parameters) {
        if (partitionsProperties.isEmpty()) {
            return null;
        }
        PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
            tableInfo,
            partitionsProperties,
            parameters
        );
        if (tableInfo.partitions().contains(partitionName) == false) {
            throw new IllegalArgumentException("Referenced partition \"" + partitionName + "\" does not exist.");
        }
        return partitionName;
    }
}
