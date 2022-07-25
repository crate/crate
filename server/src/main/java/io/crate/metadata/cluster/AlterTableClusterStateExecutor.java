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

import static org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService.maybeUpdateClusterBlock;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.elasticsearch.index.IndexSettings.same;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.ShardLimitValidator;

import io.crate.analyze.TableParameters;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Maps;
import io.crate.execution.ddl.tables.AlterTableRequest;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfoFactory;

public class AlterTableClusterStateExecutor extends DDLClusterStateTaskExecutor<AlterTableRequest> {

    private static final IndicesOptions FIND_OPEN_AND_CLOSED_INDICES_IGNORE_UNAVAILABLE_AND_NON_EXISTING = IndicesOptions.fromOptions(
        true, true, true, true);

    private final MetadataMappingService metadataMappingService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final IndexScopedSettings indexScopedSettings;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final ShardLimitValidator shardLimitValidator;
    private final NodeContext nodeContext;

    public AlterTableClusterStateExecutor(MetadataMappingService metadataMappingService,
                                          IndicesService indicesService,
                                          AllocationService allocationService,
                                          IndexScopedSettings indexScopedSettings,
                                          MetadataCreateIndexService metadataCreateIndexService,
                                          ShardLimitValidator shardLimitValidator,
                                          NodeContext nodeContext) {
        this.metadataMappingService = metadataMappingService;
        this.indicesService = indicesService;
        this.indexScopedSettings = indexScopedSettings;
        this.allocationService = allocationService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.shardLimitValidator = shardLimitValidator;
        this.nodeContext = nodeContext;
    }

    @Override
    protected ClusterState execute(ClusterState currentState, AlterTableRequest request) throws Exception {
        if (request.isPartitioned()) {
            if (request.partitionIndexName() != null) {
                assert request.mappingDelta() == null
                    : "Cannot add column to a single partition. Template and index mappings must stay in sync";
                Index[] concreteIndices = resolveIndices(currentState, request.partitionIndexName());
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
                                                         metadataCreateIndexService),
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
                    supportedSettings.add(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS);

                    currentState = updateSettings(currentState, filterSettings(request.settings(), supportedSettings), concreteIndices);
                    currentState = updateMapping(currentState, request, concreteIndices);
                }
            }
        } else {
            Index[] concreteIndices = resolveIndices(currentState, request.tableIdent().indexNameOrAlias());
            currentState = updateMapping(currentState, request, concreteIndices);
            currentState = updateSettings(currentState, request.settings(), concreteIndices);
        }

        // ensure the new table can still be parsed into a DocTableInfo to avoid breaking the table.
        new DocTableInfoFactory(nodeContext).create(request.tableIdent(), currentState);

        return currentState;
    }


    private ClusterState updateMapping(ClusterState currentState, AlterTableRequest request, Index[] concreteIndices) throws Exception {
        if (request.mappingDelta() == null) {
            return currentState;
        }
        Map<Index, MapperService> indexMapperServices = new HashMap<>();
        Map<String, Object> currentMeta = new HashMap<>();
        for (Index index : concreteIndices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
            Map<String, Object> sourceAsMap = indexMetadata.mapping().sourceAsMap();
            Map<String, Object> meta = (Map<String, Object>) sourceAsMap.get("_meta");
            if (meta != null) {
                Maps.extendRecursive(currentMeta, meta);
            }
            if (indexMapperServices.containsKey(indexMetadata.getIndex()) == false) {
                MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
                indexMapperServices.put(index, mapperService);
                // add mappings for all types, we need them for cross-type validation
                mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
            }
        }

        String mappingDelta = addExistingMeta(request, currentMeta);
        PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest(mappingDelta)
            .ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .indices(concreteIndices);

        return metadataMappingService.putMappingExecutor.applyRequest(currentState, updateRequest, indexMapperServices);
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    static String addExistingMeta(AlterTableRequest request, Map<String, Object> currentMeta) throws IOException {
        // The putMappingExtractor doesn't contain logic to merge _meta
        // and we need to preserve existing information.
        //
        // _meta format:
        //   {
        //       primary_keys: [],
        //       partitioned_by: []     -- items are a tuple of (column_name, type_name)
        //       constraints: {
        //           not_null : [],
        //       },
        //       indices: {
        //          <index_name>: <options>
        //      }
        //       check_constraints: {
        //           <constraint_name>: <expression>
        //       }
        //   }
        //
        //   - partitioned_by cannot be changed
        //
        //   - primary_keys are always additive
        //      - If present in delta, ALL keys are present (We should change this to gain atomicity)
        //
        //   - not_null constraints are always additive
        //      - If present in delta, NEW columns are present
        //
        //   - indices can only be added
        //      - If present in delta, NEW indices are present
        //
        //   - check_constraints can be added OR removed
        //      If removed, delta contains *ALL* but the one to remove
        //      If added:
        //          Empty if new column doesn't have a constraint
        //          All constraints present if new column has a constraint
        //          (We should change this to gain atomicity)
        //
        // Why is everything behaving differently? Because reasons 🤷
        // This would be simpler if we distinguished between additions and deletions in the AlterTableRequest
        Map<String, Object> mappingDeltaAsMap = request.mappingDeltaAsMap();
        Map<String, Object> metaDelta = (Map<String, Object>) mappingDeltaAsMap.get("_meta");
        if (metaDelta != null) {
            var curPartitionedBy = currentMeta.get("partitioned_by");
            if (curPartitionedBy != null) {
                metaDelta.put("partitioned_by", curPartitionedBy);
            }
            var curPrimaryKeys = currentMeta.get("primary_keys");
            if (curPrimaryKeys != null) {
                metaDelta.putIfAbsent("primary_keys", curPrimaryKeys);
            }

            Map<String, Object> checkConstraints = (Map<String, Object>) metaDelta.get("check_constraints");
            if (checkConstraints == null || checkConstraints.isEmpty()) {
                var curCheckConstraints = currentMeta.get("check_constraints");
                if (curCheckConstraints != null) {
                    metaDelta.put("check_constraints", curCheckConstraints);
                }
            }

            var curConstraints = currentMeta.get("constraints");
            if (curConstraints != null) {
                metaDelta.merge(
                    "constraints",
                    curConstraints,
                    (delta, current) -> {
                        Maps.extendRecursive((Map<String, Object>) delta, (Map<String, Object>) current);
                        return delta;
                    }
                );
            }

            var curIndices = currentMeta.get("indices");
            if (curIndices != null) {
                metaDelta.merge(
                    "indices",
                    curIndices,
                    (delta, current) -> {
                        Maps.extendRecursive((Map<String, Object>) delta, (Map<String, Object>) current);
                        return delta;
                    }
                );
            }

            var curGeneratedColumns = currentMeta.get("generated_columns");
            if (curGeneratedColumns != null) {
                metaDelta.merge(
                    "generated_columns",
                    curGeneratedColumns,
                    (delta, current) -> {
                        Maps.extendRecursive((Map<String, Object>) delta, (Map<String, Object>) current);
                        return delta;
                    }
                );
            }

            if (metaDelta.containsKey("routing")) {
                throw new IllegalArgumentException("Requested to change the routing column to " +
                                                   metaDelta.get("routing") +
                                                   ", but routing columns cannot be changed");
            }
            var curRouting = currentMeta.get("routing");
            if (curRouting != null) {
                metaDelta.put("routing", curRouting);
            }
        }
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.map(mappingDeltaAsMap);
        String mappingDelta = XContentHelper.convertToJson(BytesReference.bytes(builder), XContentType.JSON);
        return mappingDelta;
    }

    /**
     * The logic is taken over from {@link MetadataUpdateSettingsService#updateSettings(UpdateSettingsClusterStateUpdateRequest, ActionListener)}
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
                skippedSettings.add(key.replace("index.", ""));
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();

        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        // allow to change any settings to a close index, and only allow dynamic settings to be changed
        // on an open index
        Set<Index> openIndices = new HashSet<>();
        Set<Index> closeIndices = new HashSet<>();
        final String[] actualIndices = new String[concreteIndices.length];
        for (int i = 0; i < concreteIndices.length; i++) {
            Index index = concreteIndices[i];
            actualIndices[i] = index.getName();
            final IndexMetadata metadata = currentState.metadata().getIndexSafe(index);
            if (metadata.getState() == IndexMetadata.State.OPEN) {
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

        if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(openSettings)) {
            final int updatedNumberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(openSettings);
            // Verify that this won't take us over the cluster shard limit.
            int totalNewShards = openIndices.stream()
                .mapToInt(i -> MetadataUpdateSettingsService.getTotalNewShards(i, currentState, updatedNumberOfReplicas))
                .sum();
            Optional<String> error = shardLimitValidator.checkShardLimit(totalNewShards, currentState);
            if (error.isPresent()) {
                ValidationException ex = new ValidationException();
                ex.addValidationError(error.get());
                throw ex;
            }
            /*
             * We do not update the in-sync allocation IDs as they will be removed upon the first index operation which makes
             * these copies stale.
             *
             * TODO: should we update the in-sync allocation IDs once the data is deleted by the node?
             */
            routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
            metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
        }

        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetadata.INDEX_READ_ONLY_BLOCK,
                                IndexMetadata.INDEX_READ_ONLY_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                                IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetadata.INDEX_METADATA_BLOCK,
                                IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetadata.INDEX_WRITE_BLOCK,
                                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
                                openSettings);
        maybeUpdateClusterBlock(actualIndices,
                                blocks,
                                IndexMetadata.INDEX_READ_BLOCK,
                                IndexMetadata.INDEX_BLOCKS_READ_SETTING,
                                openSettings);

        if (!openIndices.isEmpty()) {
            for (Index index : openIndices) {
                IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                Settings.Builder updates = Settings.builder();
                Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                    Settings finalSettings = indexSettings.build();
                    indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) ==
                                                                           false), true);
                    metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                }
            }
        }

        if (!closeIndices.isEmpty()) {
            for (Index index : closeIndices) {
                IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                Settings.Builder updates = Settings.builder();
                Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                    Settings finalSettings = indexSettings.build();
                    indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) ==
                                                                           false), true);
                    metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                }
            }
        }

        // increment settings versions
        for (final String index : actualIndices) {
            if (same(currentState.metadata().index(index).getSettings(), metadataBuilder.get(index).getSettings()) ==
                false) {
                final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                builder.settingsVersion(1 + builder.settingsVersion());
                metadataBuilder.put(builder);
            }
        }

        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(
            routingTableBuilder.build()).blocks(blocks).build();

        // now, reroute in case things change that require it (like number of replicas)
        updatedState = allocationService.reroute(updatedState, "settings update");
        try {
            for (Index index : openIndices) {
                final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
            }
            for (Index index : closeIndices) {
                final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                // Verifies that the current index settings can be updated with the updated dynamic settings.
                indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
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

        IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(templateName);
        IndexTemplateMetadata newIndexTemplateMetadata = DDLClusterStateHelpers.updateTemplate(
            indexTemplateMetadata,
            newMapping,
            Collections.emptyMap(),
            newSetting,
            settingsValidator,
            k -> indexScopedSettings.isPrivateSetting(k) == false
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
