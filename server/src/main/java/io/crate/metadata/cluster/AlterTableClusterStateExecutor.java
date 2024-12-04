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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.TableParameters;
import io.crate.exceptions.RelationUnknown;
import io.crate.execution.ddl.tables.AlterTableRequest;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.sql.tree.ColumnPolicy;

public class AlterTableClusterStateExecutor extends DDLClusterStateTaskExecutor<AlterTableRequest> {

    private static final IndicesOptions FIND_OPEN_AND_CLOSED_INDICES_IGNORE_UNAVAILABLE_AND_NON_EXISTING = IndicesOptions.fromOptions(
        true, true, true, true);

    private final IndexScopedSettings indexScopedSettings;
    private final NodeContext nodeContext;
    private final MetadataUpdateSettingsService updateSettingsService;

    public AlterTableClusterStateExecutor(IndicesService indicesService,
                                          IndexScopedSettings indexScopedSettings,
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
        RelationName tableName = request.tableIdent();
        RelationMetadata.Table table = metadata.getRelation(tableName);
        if (table == null) {
            throw new RelationUnknown(tableName);
        }

        Metadata.Builder newMetadata = Metadata.builder(metadata);

        String partitionIndexName = request.partitionIndexName();
        List<String> partitionValues = partitionIndexName == null
            ? List.of()
            : PartitionName.fromIndexOrTemplate(partitionIndexName).values();

        if (partitionValues.isEmpty()) {
            newMetadata.addTable(
                table.name(),
                table.columns(),
                Settings.builder()
                    .put(table.settings())
                    .put(settings)
                    .build(),
                table.routingColumn(),
                columnPolicy == null ? table.columnPolicy() : columnPolicy,
                table.pkConstraintName(),
                table.checkConstraints(),
                table.primaryKeys(),
                table.partitionedBy(),
                table.state(),
                table.indexUUIDs()
            );
        } else {
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

        for (IndexMetadata indexMetadata : metadata.getIndices(tableName, partitionValues, x -> x)) {
            Settings.Builder newSettings = Settings.builder()
                .put(indexMetadata.getSettings())
                .put(settings);
            newMetadata.put(
                IndexMetadata.builder(indexMetadata).settings(newSettings).build(),
                true
            );
        }

        Metadata updatedMetadata = newMetadata.build();
        new DocTableInfoFactory(nodeContext).create(tableName, updatedMetadata);

        return ClusterState.builder(currentState)
            .metadata(updatedMetadata)
            .build();
    }

    /**
     * The logic is taken over from {@link MetadataUpdateSettingsService#updateSettings(UpdateSettingsRequest, ActionListener)}
     */
    private ClusterState updateSettings(final ClusterState currentState, final Settings settings, Index[] concreteIndices) {
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
            concreteIndices,
            skippedSettings,
            closedSettings,
            openSettings
        );
    }

    private static void validateSettings(String name,
                                         Settings settings,
                                         IndexScopedSettings indexScopedSettings,
                                         MetadataCreateIndexService metadataCreateIndexService) {
        List<String> validationErrors = new ArrayList<>();
        try {
            indexScopedSettings.validate(settings, true); // templates must be consistent with regards to dependencies
        } catch (IllegalArgumentException iae) {
            validationErrors.add(iae.getMessage());
            for (Throwable t : iae.getSuppressed()) {
                validationErrors.add(t.getMessage());
            }
        }
        List<String> indexSettingsValidation = metadataCreateIndexService.getIndexSettingsValidationErrors(settings, true);
        validationErrors.addAll(indexSettingsValidation);
        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(name, validationException.getMessage());
        }
    }

    @VisibleForTesting
    static Settings filterSettings(Settings settings, List<Setting<?>> settingsFilter) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Setting<?> setting : settingsFilter) {
            if (setting.isGroupSetting()) {
                String prefix = setting.getKey(); // getKey() returns prefix for a group setting
                var settingsGroup = settings.getByPrefix(prefix);
                for (String name : settingsGroup.keySet()) {
                    settingsBuilder.put(prefix + name, settingsGroup.get(name)); // No dot added as prefix already has dot at the end.
                }
            } else {
                String value = settings.get(setting.getKey());
                if (value != null) {
                    settingsBuilder.put(setting.getKey(), value);
                }
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
