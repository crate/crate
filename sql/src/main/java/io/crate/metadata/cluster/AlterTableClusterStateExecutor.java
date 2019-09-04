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

package io.crate.metadata.cluster;

import com.google.common.annotations.VisibleForTesting;
import io.crate.Constants;
import io.crate.analyze.TableParameters;
import io.crate.execution.ddl.tables.AlterTableRequest;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService.maybeUpdateClusterBlock;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.elasticsearch.index.IndexSettings.same;

public class AlterTableClusterStateExecutor extends DDLClusterStateTaskExecutor<AlterTableRequest> {

    private static final IndicesOptions FIND_OPEN_AND_CLOSED_INDICES_IGNORE_UNAVAILABLE_AND_NON_EXISTING = IndicesOptions.fromOptions(
        true, true, true, true);

    private final MetaDataMappingService metaDataMappingService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final MetaDataCreateIndexService metaDataCreateIndexService;

    public AlterTableClusterStateExecutor(MetaDataMappingService metaDataMappingService,
                                          IndicesService indicesService,
                                          AllocationService allocationService,
                                          IndexScopedSettings indexScopedSettings,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          MetaDataCreateIndexService metaDataCreateIndexService) {
        this.metaDataMappingService = metaDataMappingService;
        this.indicesService = indicesService;
        this.indexScopedSettings = indexScopedSettings;
        this.allocationService = allocationService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.metaDataCreateIndexService = metaDataCreateIndexService;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, AlterTableRequest request) throws Exception {
        if (request.isPartitioned()) {
            if (request.partitionIndexName() != null) {
                Index[] concreteIndices = resolveIndices(currentState, request.partitionIndexName());
                currentState = updateMapping(currentState, request, concreteIndices);
                currentState = updateSettings(currentState, request.settings(), concreteIndices);
            } else {
                // template gets all changes unfiltered
                currentState = updateTemplate(
                    currentState,
                    request.tableIdent(),
                    request.settings(),
                    request.mappingDeltaAsMap(),
                    (name, settings) -> validateSettings(name,
                                                         settings,
                                                         indexScopedSettings,
                                                         metaDataCreateIndexService),
                    indexScopedSettings);

                if (!request.excludePartitions()) {
                    Index[] concreteIndices = resolveIndices(currentState, request.tableIdent().indexNameOrAlias());

                    // These settings only apply for already existing partitions
                    List<String> supportedSettings = TableParameters.PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
                        .supportedSettings()
                        .values()
                        .stream()
                        .map(Setting::getKey)
                        .collect(Collectors.toList());

                    // auto_expand_replicas must be explicitly added as it is hidden under NumberOfReplicasSetting
                    supportedSettings.add(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS);

                    currentState = updateSettings(currentState, filterSettings(request.settings(), supportedSettings), concreteIndices);
                    currentState = updateMapping(currentState, request, concreteIndices);
                }
            }
        } else {
            Index[] concreteIndices = resolveIndices(currentState, request.tableIdent().indexNameOrAlias());
            currentState = updateMapping(currentState, request, concreteIndices);
            currentState = updateSettings(currentState, request.settings(), concreteIndices);
        }

        return currentState;
    }


    private ClusterState updateMapping(ClusterState currentState, AlterTableRequest request, Index[] concreteIndices) throws Exception {
        if (request.mappingDelta() == null) {
            return currentState;
        }
        Map<Index, MapperService> indexMapperServices = new HashMap<>();
        for (Index index : concreteIndices) {
            final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
            if (indexMapperServices.containsKey(indexMetaData.getIndex()) == false) {
                MapperService mapperService = indicesService.createIndexMapperService(indexMetaData);
                indexMapperServices.put(index, mapperService);
                // add mappings for all types, we need them for cross-type validation
                mapperService.merge(indexMetaData, MapperService.MergeReason.MAPPING_RECOVERY, false);
            }
        }

        PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest()
            .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
            .indices(concreteIndices).type(Constants.DEFAULT_MAPPING_TYPE)
            .source(request.mappingDelta());

        return metaDataMappingService.putMappingExecutor.applyRequest(currentState, updateRequest, indexMapperServices);
    }

    /**
     * The logic is taken over from {@link MetaDataUpdateSettingsService#updateSettings(UpdateSettingsClusterStateUpdateRequest, ActionListener)}
     */
    private ClusterState updateSettings(final ClusterState currentState, final Settings settings, Index[] concreteIndices) {

        final Settings normalizedSettings = Settings.builder()
            .put(markArchivedSettings(settings))
            .normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX)
            .build();

        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        for (String key : normalizedSettings.keySet()) {
            Setting setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                   || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " +
                  normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        final MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
        // allow to change any settings to a close index, and only allow dynamic settings to be changed
        // on an open index
        Set<Index> openIndices = new HashSet<>();
        Set<Index> closeIndices = new HashSet<>();
        final String[] actualIndices = new String[concreteIndices.length];
        for (int i = 0; i < concreteIndices.length; i++) {
            Index index = concreteIndices[i];
            actualIndices[i] = index.getName();
            final IndexMetaData metaData = currentState.metaData().getIndexSafe(index);
            if (metaData.getState() == IndexMetaData.State.OPEN) {
                openIndices.add(index);
            } else {
                closeIndices.add(index);
            }
        }

        if (!skippedSettings.isEmpty() && !openIndices.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                                                             "Can't update non dynamic settings [%s] for open indices %s",
                                                             skippedSettings,
                                                             openIndices));
        }

        int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, -1);

        if (updatedNumberOfReplicas != -1) {
            // we do *not* update the in sync allocation ids as they will be removed upon the first index
            // operation which make these copies stale
            // TODO: update the list once the data is deleted by the node?
            routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
            metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
        }

        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetaData.INDEX_READ_ONLY_BLOCK,
                                IndexMetaData.INDEX_READ_ONLY_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetaData.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                                IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetaData.INDEX_METADATA_BLOCK,
                                IndexMetaData.INDEX_BLOCKS_METADATA_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetaData.INDEX_WRITE_BLOCK,
                                IndexMetaData.INDEX_BLOCKS_WRITE_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetaData.INDEX_READ_BLOCK,
                                IndexMetaData.INDEX_BLOCKS_READ_SETTING,
                                openSettings);

        if (!openIndices.isEmpty()) {
            for (Index index : openIndices) {
                IndexMetaData indexMetaData = metaDataBuilder.getSafe(index);
                Settings.Builder updates = Settings.builder();
                Settings.Builder indexSettings = Settings.builder().put(indexMetaData.getSettings());
                if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                    Settings finalSettings = indexSettings.build();
                    indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) ==
                                                                           false), true);
                    metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(finalSettings));
                }
            }
        }

        if (!closeIndices.isEmpty()) {
            for (Index index : closeIndices) {
                IndexMetaData indexMetaData = metaDataBuilder.getSafe(index);
                Settings.Builder updates = Settings.builder();
                Settings.Builder indexSettings = Settings.builder().put(indexMetaData.getSettings());
                if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                    Settings finalSettings = indexSettings.build();
                    indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) ==
                                                                           false), true);
                    metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(finalSettings));
                }
            }
        }

        // increment settings versions
        for (final String index : actualIndices) {
            if (same(currentState.metaData().index(index).getSettings(), metaDataBuilder.get(index).getSettings()) ==
                false) {
                final IndexMetaData.Builder builder = IndexMetaData.builder(metaDataBuilder.get(index));
                builder.settingsVersion(1 + builder.settingsVersion());
                metaDataBuilder.put(builder);
            }
        }

        ClusterState updatedState = ClusterState.builder(currentState).metaData(metaDataBuilder).routingTable(
            routingTableBuilder.build()).blocks(blocks).build();

        // now, reroute in case things change that require it (like number of replicas)
        updatedState = allocationService.reroute(updatedState, "settings update");
        try {
            for (Index index : openIndices) {
                final IndexMetaData currentMetaData = currentState.getMetaData().getIndexSafe(index);
                final IndexMetaData updatedMetaData = updatedState.metaData().getIndexSafe(index);
                indicesService.verifyIndexMetadata(currentMetaData, updatedMetaData);
            }
            for (Index index : closeIndices) {
                final IndexMetaData currentMetaData = currentState.getMetaData().getIndexSafe(index);
                final IndexMetaData updatedMetaData = updatedState.metaData().getIndexSafe(index);
                // Verifies that the current index settings can be updated with the updated dynamic settings.
                indicesService.verifyIndexMetadata(currentMetaData, updatedMetaData);
                // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                indicesService.verifyIndexMetadata(updatedMetaData, updatedMetaData);
            }
        } catch (IOException ex) {
            throw ExceptionsHelper.convertToElastic(ex);
        }
        return updatedState;
    }

    static ClusterState updateTemplate(ClusterState currentState,
                                       RelationName relationName,
                                       Settings newSetting,
                                       Map<String, Object> newMapping,
                                       BiConsumer<String, Settings> settingsValidator,
                                       IndexScopedSettings indexScopedSettings) throws IOException {

        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());

        IndexTemplateMetaData indexTemplateMetaData = currentState.metaData().templates().get(templateName);
        IndexTemplateMetaData newIndexTemplateMetaData = DDLClusterStateHelpers.updateTemplate(
            indexTemplateMetaData,
            newMapping,
            Collections.emptyMap(),
            newSetting,
            settingsValidator,
            k -> indexScopedSettings.isPrivateSetting(k) == false
            );

        final MetaData.Builder metaData = MetaData.builder(currentState.metaData()).put(newIndexTemplateMetaData);
        return ClusterState.builder(currentState).metaData(metaData).build();
    }

    private static void validateSettings(String name,
                                         Settings settings,
                                         IndexScopedSettings indexScopedSettings,
                                         MetaDataCreateIndexService metaDataCreateIndexService) {
        List<String> validationErrors = new ArrayList<>();
        try {
            indexScopedSettings.validate(settings, true); // templates must be consistent with regards to dependencies
        } catch (IllegalArgumentException iae) {
            validationErrors.add(iae.getMessage());
            for (Throwable t : iae.getSuppressed()) {
                validationErrors.add(t.getMessage());
            }
        }
        List<String> indexSettingsValidation = metaDataCreateIndexService.getIndexSettingsValidationErrors(settings, true);
        validationErrors.addAll(indexSettingsValidation);
        if (!validationErrors.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(name, validationException.getMessage());
        }
    }

    private Settings filterSettings(Settings settings, List<String> settingsFilter) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (String settingName : settingsFilter) {
            String setting = settings.get(settingName);
            if (setting != null) {
                settingsBuilder.put(settingName, setting);
            }
        }
        return settingsBuilder.build();
    }

    private Index[] resolveIndices(ClusterState currentState, String indexExpressions) {
        return indexNameExpressionResolver.concreteIndices(currentState,
                                                           FIND_OPEN_AND_CLOSED_INDICES_IGNORE_UNAVAILABLE_AND_NON_EXISTING, indexExpressions);
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
