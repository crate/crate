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

package io.crate.metadata.cluster;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.Index;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.TableParameters;
import io.crate.execution.ddl.tables.AlterTableRequest;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.SchemaInfo;
import io.crate.sql.tree.ColumnPolicy;

public class AlterTableClusterStateExecutor extends DDLClusterStateTaskExecutor<AlterTableRequest> {

    private final IndexScopedSettings indexScopedSettings;
    private final NodeContext nodeContext;
    private final MetadataUpdateSettingsService updateSettingsService;

    public AlterTableClusterStateExecutor(IndexScopedSettings indexScopedSettings,
                                          MetadataUpdateSettingsService updateSettingsService,
                                          NodeContext nodeContext) {
        this.indexScopedSettings = indexScopedSettings;
        this.nodeContext = nodeContext;
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, AlterTableRequest request) throws Exception {
        String policy = request.settings().get(TableParameters.COLUMN_POLICY.getKey());
        ColumnPolicy columnPolicy = policy == null ? null : ColumnPolicy.of(policy);

        Settings.Builder settingsBuilder = Settings.builder()
            .put(request.settings());
        settingsBuilder.remove(TableParameters.COLUMN_POLICY.getKey());
        Settings settings = settingsBuilder.build();

        Metadata metadata = currentState.metadata();
        RelationMetadata relation = metadata.getRelation(request.tableIdent());
        if (request.partitionValues().isEmpty() && relation instanceof RelationMetadata.Table table) {
            Metadata.Builder newMetadata = Metadata.builder(metadata);
            Builder newSettings = Settings.builder()
                .put(table.settings())
                .put(settings);
            for (String setting : settings.keySet()) {
                if (!settings.hasValue(setting)) {
                    newSettings.remove(setting);
                }
            }
            newMetadata.setTable(
                table.name(),
                table.columns(),
                newSettings.build(),
                table.routingColumn(),
                columnPolicy == null ? table.columnPolicy() : columnPolicy,
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                table.indexUUIDs(),
                table.tableVersion() + 1
            );
            currentState = ClusterState.builder(currentState)
                .metadata(newMetadata)
                .build();
        } else if (!request.partitionValues().isEmpty()) {
            for (var tableOnlySetting : TableParameters.TABLE_ONLY_SETTINGS) {
                if (tableOnlySetting.exists(settings)) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "\"%s\" cannot be changed on partition level",
                        tableOnlySetting.getKey()
                    ));
                }
            }
        }
        List<Index> concreteIndices = metadata.getIndices(
            request.tableIdent(),
            request.partitionValues(),
            false,
            IndexMetadata::getIndex
        );
        List<PartitionName> partitions = partitions(request);
        if (request.isPartitioned()) {
            if (!request.partitionValues().isEmpty()) {
                currentState = updateSettings(currentState, settings, partitions);
            } else {
                // using settings from request with column policy still present
                Map<String, Object> newMapping = settingsToMapping(request.settings());

                // template gets all changes unfiltered
                currentState = updateTemplate(
                    currentState,
                    request.tableIdent(),
                    settings,
                    newMapping,
                    indexScopedSettings
                );

                if (!request.excludePartitions()) {
                    // These settings only apply for already existing partitions
                    List<Setting<?>> supportedSettings = new ArrayList<>(
                        TableParameters.PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
                            .supportedSettings()
                            .values());

                    // auto_expand_replicas must be explicitly added as it is hidden under NumberOfReplicasSetting
                    supportedSettings.add(AutoExpandReplicas.SETTING);

                    currentState = updateSettings(currentState, filterSettings(settings, supportedSettings), partitions);
                    currentState = updateMapping(currentState, concreteIndices, columnPolicy);
                }
            }
        } else {
            currentState = updateMapping(currentState, concreteIndices, columnPolicy);
            currentState = updateSettings(currentState, settings, partitions);
        }

        // ensure the new table can still be parsed into a Doc|BlobTableInfo to avoid breaking the table.
        RelationName relationName = request.tableIdent();
        SchemaInfo schemaInfo = nodeContext.schemas().getOrCreateSchemaInfo(relationName.schema());
        schemaInfo.create(relationName, currentState.metadata());

        return currentState;
    }

    private List<PartitionName> partitions(AlterTableRequest request) {
        if (request.isPartitioned()) {
            return List.of(new PartitionName(request.tableIdent(), request.partitionValues()));
        } else {
            return List.of(new PartitionName(request.tableIdent(), List.of()));
        }
    }

    private ClusterState updateMapping(ClusterState currentState,
                                       List<Index> concreteIndices,
                                       @Nullable ColumnPolicy columnPolicy) throws IOException {
        if (columnPolicy == null) {
            return currentState;
        }
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            String mappingValue = columnPolicy.toMappingValue();
            if (indexMapping.get(ColumnPolicy.MAPPING_KEY).equals(mappingValue)) {
                return currentState;
            }
            indexMapping.put(ColumnPolicy.MAPPING_KEY, mappingValue);
            IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
            imBuilder.putMapping(new MappingMetadata(indexMapping)).mappingVersion(1 + imBuilder.mappingVersion());
            metadataBuilder.put(imBuilder); // implicitly increments metadata version.
        }

        return ClusterState.builder(currentState).metadata(metadataBuilder).build();
    }


    /**
     * The logic is taken over from {@link MetadataUpdateSettingsService#updateSettings(UpdateSettingsRequest, ActionListener)}
     */
    private ClusterState updateSettings(final ClusterState currentState, final Settings settings, List<PartitionName> partitions) {
        final Settings normalizedSettings = Settings.builder()
            .put(markArchivedSettings(settings))
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();

        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        for (String key : normalizedSettings.keySet()) {
            Setting<?> setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                   || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " +
                  normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key.replace("index.", ""));
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        return updateSettingsService.updateState(
            currentState,
            partitions,
            skippedSettings,
            closedSettings,
            openSettings
        );
    }

    public static Map<String, Object> settingsToMapping(Settings settings) {
        String policy = settings.get(TableParameters.COLUMN_POLICY.getKey());
        if (policy == null) {
            return Map.of();
        }
        return Map.of(ColumnPolicy.MAPPING_KEY, ColumnPolicy.of(policy).toMappingValue());
    }

    static ClusterState updateTemplate(ClusterState currentState,
                                       RelationName relationName,
                                       Settings newSettings,
                                       Map<String, Object> newMapping,
                                       IndexScopedSettings indexScopedSettings) {

        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);
        IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
            indexTemplateMetadata,
            newMapping,
            Collections.emptyMap(),
            newSettings,
            indexScopedSettings
        );

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata()).put(newIndexTemplateMetadata);
        return ClusterState.builder(currentState).metadata(metadata).build();
    }

    @VisibleForTesting
    static Settings filterSettings(Settings settings, List<Setting<?>> settingsFilter) {
        Settings.Builder settingsBuilder = Settings.builder();
        Set<String> settingNames = settings.keySet();
        for (Setting<?> setting : settingsFilter) {
            String key = setting.getKey();
            if (setting.isGroupSetting()) {
                var settingsGroup = settings.getByPrefix(key);
                for (String name : settingsGroup.keySet()) {
                    settingsBuilder.put(key + name, settingsGroup.get(name)); // No dot added as prefix already has dot at the end.
                }
            } else if (settingNames.contains(key)) {
                settingsBuilder.put(key, settings.get(key));
            }
        }
        return settingsBuilder.build();
    }

    /**
     * Mark possible archived settings to be removed, they are not allowed to be written.
     * (Private settings are already filtered out later at the meta data update service.)
     */
    @VisibleForTesting
    static Settings markArchivedSettings(Settings settings) {
        return Settings.builder()
            .put(settings)
            .putNull(ARCHIVED_SETTINGS_PREFIX + "*")
            .build();
    }
}
