/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import static org.elasticsearch.index.IndexSettings.same;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetadataUpdateSettingsService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataUpdateSettingsService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;

    @Inject
    public MetadataUpdateSettingsService(ClusterService clusterService,
                                         AllocationService allocationService,
                                         IndexScopedSettings indexScopedSettings,
                                         IndicesService indicesService,
                                         ShardLimitValidator shardLimitValidator) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
        this.shardLimitValidator = shardLimitValidator;
    }

    public void updateSettings(final UpdateSettingsRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final Settings normalizedSettings = Settings.builder()
            .put(request.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(
            normalizedSettings,
            false, // don't validate dependencies here we check it below never allow to change the number of shards
            true // validate internal or private index settings
        );
        for (String key : normalizedSettings.keySet()) {
            Setting<?> setting = indexScopedSettings.get(key);
            assert setting != null // we already validated the normalized settings
                : "unknown setting: " + key + " hasValue: " + normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key.replace("index.", ""));
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> updateTask = new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Index[] concreteIndices = IndexNameExpressionResolver.concreteIndices(currentState, request);
                return updateState(
                    currentState,
                    concreteIndices,
                    skippedSettings,
                    closedSettings,
                    openSettings,
                    preserveExisting
                );
            }
        };
        clusterService.submitStateUpdateTask("update-settings", updateTask);
    }

    public ClusterState updateState(ClusterState currentState,
                                    Index[] concreteIndices,
                                    final Set<String> skippedSettings,
                                    final Settings closedSettings,
                                    final Settings openSettings,
                                    final boolean preserveExisting) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

        // allow to change any settings to a close index, and only allow dynamic
        // settings to be changed
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
                    "Can't update non dynamic settings [%s] for open indices %s", skippedSettings, openIndices));
        }

        if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(openSettings)) {
            final int updatedNumberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(openSettings);
            if (preserveExisting == false) {
                // Verify that this won't take us over the cluster shard limit.
                int totalNewShards = Arrays.stream(concreteIndices)
                        .mapToInt(i -> getTotalNewShards(i, currentState, updatedNumberOfReplicas))
                        .sum();
                Optional<String> error = shardLimitValidator.checkShardLimit(totalNewShards, currentState);
                if (error.isPresent()) {
                    ValidationException ex = new ValidationException();
                    ex.addValidationError(error.get());
                    throw ex;
                }

                /*
                 * We do not update the in-sync allocation IDs as they will be removed upon the
                 * first index operation which makes
                 * these copies stale.
                 *
                 * TODO: should we update the in-sync allocation IDs once the data is deleted by
                 * the node?
                 */
                routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                LOGGER.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas,
                        actualIndices);
            }
        }

        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_ONLY_BLOCK,
                IndexMetadata.INDEX_READ_ONLY_SETTING, openSettings);
        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING, openSettings);
        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_METADATA_BLOCK,
                IndexMetadata.INDEX_BLOCKS_METADATA_SETTING, openSettings);
        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_WRITE_BLOCK,
                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING, openSettings);
        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_BLOCK,
                IndexMetadata.INDEX_BLOCKS_READ_SETTING, openSettings);

        for (Index index : openIndices) {
            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
            Settings.Builder updates = Settings.builder();
            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
            if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                if (preserveExisting) {
                    indexSettings.put(indexMetadata.getSettings());
                }
                Settings finalSettings = indexSettings.build();
                indexScopedSettings.validate(
                        finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
            }
        }

        for (Index index : closeIndices) {
            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
            Settings.Builder updates = Settings.builder();
            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
            if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                if (preserveExisting) {
                    indexSettings.put(indexMetadata.getSettings());
                }
                Settings finalSettings = indexSettings.build();
                indexScopedSettings.validate(
                        finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
            }
        }

        // increment settings versions
        for (final String index : actualIndices) {
            if (same(currentState.metadata().index(index).getSettings(),
                    metadataBuilder.get(index).getSettings()) == false) {
                final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                builder.settingsVersion(1 + builder.settingsVersion());
                metadataBuilder.put(builder);
            }
        }

        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder)
                .routingTable(routingTableBuilder.build()).blocks(blocks).build();

        // now, reroute in case things change that require it (like number of replicas)
        updatedState = allocationService.reroute(updatedState, "settings update");
        try {
            for (Index index : openIndices) {
                final IndexMetadata currentMetadata = currentState.metadata().getIndexSafe(index);
                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
            }
            for (Index index : closeIndices) {
                final IndexMetadata currentMetadata = currentState.metadata().getIndexSafe(index);
                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                // Verifies that the current index settings can be updated with the updated
                // dynamic settings.
                indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                // Now check that we can create the index with the updated settings (dynamic and
                // non-dynamic).
                // This step is mandatory since we allow to update non-dynamic settings on
                // closed indices.
                indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return updatedState;
    }

    public static int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        int shardsInIndex = indexMetadata.getNumberOfShards();
        int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    public static void maybeUpdateClusterBlock(String[] actualIndices, ClusterBlocks.Builder blocks, ClusterBlock block, Setting<Boolean> setting, Settings openSettings) {
        if (setting.exists(openSettings)) {
            final boolean updateBlock = setting.get(openSettings);
            for (String index : actualIndices) {
                if (updateBlock) {
                    blocks.addIndexBlock(index, block);
                } else {
                    blocks.removeIndexBlock(index, block);
                }
            }
        }
    }
}
