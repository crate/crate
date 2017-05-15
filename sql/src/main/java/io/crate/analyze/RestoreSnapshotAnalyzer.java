/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.PartitionAlreadyExistsException;
import io.crate.exceptions.TableAlreadyExistsException;
import io.crate.executor.transport.RepositoryService;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.Table;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.crate.analyze.SnapshotSettings.IGNORE_UNAVAILABLE;
import static io.crate.analyze.SnapshotSettings.WAIT_FOR_COMPLETION;

class RestoreSnapshotAnalyzer {

    private static final ImmutableMap<String, SettingsApplier> SETTINGS = ImmutableMap.<String, SettingsApplier>builder()
        .put(IGNORE_UNAVAILABLE.name(), new SettingsAppliers.BooleanSettingsApplier(IGNORE_UNAVAILABLE))
        .put(WAIT_FOR_COMPLETION.name(), new SettingsAppliers.BooleanSettingsApplier(WAIT_FOR_COMPLETION))
        .build();

    private final RepositoryService repositoryService;
    private final Schemas schemas;

    RestoreSnapshotAnalyzer(RepositoryService repositoryService, Schemas schemas) {
        this.repositoryService = repositoryService;
        this.schemas = schemas;
    }

    public RestoreSnapshotAnalyzedStatement analyze(RestoreSnapshot node, Analysis analysis) {
        List<String> nameParts = node.name().getParts();
        Preconditions.checkArgument(
            nameParts.size() == 2, "Snapshot name not supported, only <repository>.<snapshot> works.");
        String repositoryName = nameParts.get(0);
        repositoryService.failIfRepositoryDoesNotExist(repositoryName);

        // validate and extract settings
        Settings settings = GenericPropertiesConverter.settingsFromProperties(
            node.properties(), analysis.parameterContext(), SETTINGS).build();

        if (node.tableList().isPresent()) {
            List<Table> tableList = node.tableList().get();
            Set<RestoreSnapshotAnalyzedStatement.RestoreTableInfo> restoreTables = new HashSet<>(tableList.size());
            for (Table table : tableList) {
                TableIdent tableIdent = TableIdent.of(table, analysis.sessionContext().defaultSchema());
                boolean tableExists = schemas.tableExists(tableIdent);

                if (tableExists) {
                    if (table.partitionProperties().isEmpty()) {
                        throw new TableAlreadyExistsException(tableIdent);
                    }

                    DocTableInfo docTableInfo = schemas.getTableInfo(
                        tableIdent, Operation.RESTORE_SNAPSHOT, analysis.sessionContext().user());
                    PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                        tableIdent,
                        docTableInfo,
                        table.partitionProperties(),
                        analysis.parameterContext().parameters());
                    if (docTableInfo.partitions().contains(partitionName)) {
                        throw new PartitionAlreadyExistsException(partitionName);
                    }
                    restoreTables.add(new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(tableIdent, partitionName));
                } else {
                    if (table.partitionProperties().isEmpty()) {
                        restoreTables.add(new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(tableIdent, null));
                    } else {
                        PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                            tableIdent,
                            null,
                            table.partitionProperties(),
                            analysis.parameterContext().parameters());
                        restoreTables.add(new RestoreSnapshotAnalyzedStatement.RestoreTableInfo(
                            tableIdent, partitionName));
                    }
                }
            }

            return RestoreSnapshotAnalyzedStatement.forTables(
                nameParts.get(1),
                repositoryName,
                settings,
                ImmutableList.copyOf(restoreTables));
        } else {
            return RestoreSnapshotAnalyzedStatement.all(nameParts.get(1), repositoryName, settings);
        }
    }
}
