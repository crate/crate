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
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import io.crate.analyze.TableParameters;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.execution.ddl.tables.AlterTableRequest;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.sql.tree.ColumnPolicy;

public class AlterTableClusterStateExecutor extends DDLClusterStateTaskExecutor<AlterTableRequest> {

    private static final IndicesOptions FIND_OPEN_AND_CLOSED_INDICES_IGNORE_UNAVAILABLE_AND_NON_EXISTING = IndicesOptions.fromOptions(
        true, true, true, true);

    private final IndicesService indicesService;
    private final IndexScopedSettings indexScopedSettings;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final NodeContext nodeContext;
    private final MetadataUpdateSettingsService updateSettingsService;

    public AlterTableClusterStateExecutor(IndicesService indicesService,
                                          IndexScopedSettings indexScopedSettings,
                                          MetadataCreateIndexService metadataCreateIndexService,
                                          MetadataUpdateSettingsService updateSettingsService,
                                          NodeContext nodeContext) {
        this.indicesService = indicesService;
        this.indexScopedSettings = indexScopedSettings;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.nodeContext = nodeContext;
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, AlterTableRequest request) throws Exception {
        if (request.isPartitioned()) {
            if (request.partitionIndexName() != null) {
                assert request.mappingDelta() == null
                    : "Cannot SET column policy to a single partition.";
                Index[] concreteIndices = resolveIndices(currentState, request.partitionIndexName());
                currentState = updateSettings(currentState, request.settings(), concreteIndices);
            } else {
                // template gets all changes unfiltered
                currentState = updateTemplate(
                    currentState,
                    request.tableIdent(),
                    request.settings(),
                    request.mappingDeltaAsMap(), // In case of SET COLUMN column_policy delta in request will be not null and will contain a new policy.
                    (name, settings) -> validateSettings(name,
                                                         settings,
                                                         indexScopedSettings,
                                                         metadataCreateIndexService),
                    indexScopedSettings);

                if (!request.excludePartitions()) {
                    Index[] concreteIndices = resolveIndices(currentState, request.tableIdent().indexNameOrAlias());

                    // These settings only apply for already existing partitions
                    List<Setting<?>> supportedSettings = TableParameters.PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
                        .supportedSettings()
                        .values()
                        .stream()
                        .collect(Collectors.toList());

                    // auto_expand_replicas must be explicitly added as it is hidden under NumberOfReplicasSetting
                    supportedSettings.add(AutoExpandReplicas.SETTING);

                    currentState = updateSettings(currentState, filterSettings(request.settings(), supportedSettings), concreteIndices);
                    currentState = updateMapping(currentState, concreteIndices, request.mappingDeltaAsMap());
                }
            }
        } else {
            Index[] concreteIndices = resolveIndices(currentState, request.tableIdent().indexNameOrAlias());
            currentState = updateMapping(currentState, concreteIndices, request.mappingDeltaAsMap());
            currentState = updateSettings(currentState, request.settings(), concreteIndices);
        }

        // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
        new DocTableInfoFactory(nodeContext).create(request.tableIdent(), currentState.metadata());

        return currentState;
    }

    /**
     * @param mappingDelta is dynamic policy setting which for now is the only mapping change allowed by ALTER TABLE SET
     */
    private ClusterState updateMapping(ClusterState currentState,
                                       Index[] concreteIndices,
                                       Map<String, Object> mappingDelta) throws IOException {

        if (mappingDelta.isEmpty()) {
            return currentState;
        }

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);

            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            if (indexMapping.get(ColumnPolicy.MAPPING_KEY).equals(mappingDelta.get(ColumnPolicy.MAPPING_KEY))) {
                return currentState;
            }
            indexMapping.put(ColumnPolicy.MAPPING_KEY, mappingDelta.get(ColumnPolicy.MAPPING_KEY));


            MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);

            mapperService.merge(indexMapping, MapperService.MergeReason.MAPPING_UPDATE);
            DocumentMapper mapper = mapperService.documentMapper();

            IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
            imBuilder.putMapping(new MappingMetadata(mapper.mappingSource())).mappingVersion(1 + imBuilder.mappingVersion());
            metadataBuilder.put(imBuilder); // implicitly increments metadata version.
        }

        return ClusterState.builder(currentState).metadata(metadataBuilder).build();
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

        boolean preserveExisting = false;
        return updateSettingsService.updateState(
            currentState,
            concreteIndices,
            skippedSettings,
            closedSettings,
            openSettings,
            preserveExisting
        );
    }

    static ClusterState updateTemplate(ClusterState currentState,
                                       RelationName relationName,
                                       Settings newSetting,
                                       Map<String, Object> newMapping,
                                       BiConsumer<String, Settings> settingsValidator,
                                       IndexScopedSettings indexScopedSettings) throws IOException {

        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());

        IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);
        IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
            indexTemplateMetadata,
            newMapping,
            Collections.emptyMap(),
            newSetting,
            indexScopedSettings
        );

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata()).put(newIndexTemplateMetadata);
        return ClusterState.builder(currentState).metadata(metadata).build();
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

    public static Index[] resolveIndices(ClusterState currentState, String indexExpressions) {
        return IndexNameExpressionResolver.concreteIndices(
            currentState.metadata(),
            FIND_OPEN_AND_CLOSED_INDICES_IGNORE_UNAVAILABLE_AND_NON_EXISTING,
            indexExpressions
        );
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
