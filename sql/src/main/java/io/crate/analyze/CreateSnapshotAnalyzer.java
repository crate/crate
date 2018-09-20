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
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.execution.ddl.RepositoryService;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static io.crate.analyze.SnapshotSettings.IGNORE_UNAVAILABLE;
import static io.crate.analyze.SnapshotSettings.SETTINGS;

class CreateSnapshotAnalyzer {

    private static final Logger LOGGER = Loggers.getLogger(CreateSnapshotAnalyzer.class);
    private final RepositoryService repositoryService;
    private final Schemas schemas;


    CreateSnapshotAnalyzer(RepositoryService repositoryService, Schemas schemas) {
        this.repositoryService = repositoryService;
        this.schemas = schemas;
    }

    public CreateSnapshotAnalyzedStatement analyze(CreateSnapshot node, Analysis analysis) {
        Optional<QualifiedName> repositoryName = node.name().getPrefix();
        // validate repository
        Preconditions.checkArgument(repositoryName.isPresent(), "Snapshot must be specified by \"<repository_name>\".\"<snapshot_name>\"");
        Preconditions.checkArgument(repositoryName.get().getParts().size() == 1,
            String.format(Locale.ENGLISH, "Invalid repository name '%s'", repositoryName.get()));
        repositoryService.failIfRepositoryDoesNotExist(repositoryName.get().toString());

        // snapshot existence in repo is validated upon execution
        String snapshotName = node.name().getSuffix();
        Snapshot snapshot = new Snapshot(repositoryName.get().toString(), new SnapshotId(snapshotName, UUID.randomUUID().toString()));

        // validate and extract settings
        Settings settings = GenericPropertiesConverter.settingsFromProperties(
            node.properties(), analysis.parameterContext().parameters(), SETTINGS).build();

        boolean ignoreUnavailable = IGNORE_UNAVAILABLE.get(settings);

        // iterate tables
        if (!node.tableList().isEmpty()) {
            Set<String> snapshotIndices = new HashSet<>(node.tableList().size());
            for (Table table : node.tableList()) {
                DocTableInfo docTableInfo;
                try {
                    docTableInfo = (DocTableInfo) schemas.resolveTableInfo(table.getName(), Operation.CREATE_SNAPSHOT,
                        analysis.sessionContext().searchPath());
                } catch (ResourceUnknownException e) {
                    if (ignoreUnavailable) {
                        LOGGER.info("ignoring: {}", e.getMessage());
                        continue;
                    } else {
                        throw e;
                    }
                }
                if (table.partitionProperties().isEmpty()) {
                    Stream.of(docTableInfo.concreteIndices()).forEach(snapshotIndices::add);
                } else {
                    PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                        docTableInfo,
                        table.partitionProperties(),
                        analysis.parameterContext().parameters()
                    );
                    if (!docTableInfo.partitions().contains(partitionName)) {
                        if (!ignoreUnavailable) {
                            throw new PartitionUnknownException(partitionName);
                        } else {
                            LOGGER.info("ignoring unknown partition of table '{}' with ident '{}'", partitionName.relationName(), partitionName.ident());
                        }
                    } else {
                        snapshotIndices.add(partitionName.asIndexName());
                    }
                }
            }
            return CreateSnapshotAnalyzedStatement.forTables(snapshot,
                settings,
                ImmutableList.copyOf(snapshotIndices));
        } else {
            for (SchemaInfo schemaInfo : schemas) {
                for (TableInfo tableInfo : schemaInfo.getTables()) {
                    // only check for user generated tables
                    if (tableInfo instanceof DocTableInfo) {
                        Operation.blockedRaiseException(tableInfo, Operation.READ);
                    }
                }
            }
            return CreateSnapshotAnalyzedStatement.all(snapshot, settings);
        }
    }
}
