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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;

import io.crate.metadata.PartitionName;

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

        AckedClusterStateUpdateTask<ClusterStateUpdateResponse> updateTask = new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateState(
                    currentState,
                    request.partitions(),
                    skippedSettings,
                    closedSettings,
                    openSettings
                );
            }
        };
        clusterService.submitStateUpdateTask("update-settings", updateTask);
    }

    public ClusterState updateState(ClusterState currentState,
                                    List<PartitionName> partitions,
                                    final Set<String> skippedSettings,
                                    final Settings closedSettings,
                                    final Settings openSettings) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

        List<IndexMetadata> indexes = currentState.metadata().getIndices(partitions, true, Function.identity());

        List<String> openIndices = indexes.stream()
            .filter(im -> im.getState() == IndexMetadata.State.OPEN)
            .map(im -> im.getIndex().getUUID())
            .toList();
        if (!skippedSettings.isEmpty() && !openIndices.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                "Can't update non dynamic settings [%s] for open indices %s", skippedSettings, openIndices));
        }

        if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(openSettings)) {
            final int updatedNumberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(openSettings);
            // Verify that this won't take us over the cluster shard limit.
            int totalNewShards = indexes.stream()
                    .mapToInt(i -> getTotalNewShards(i, updatedNumberOfReplicas))
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
            routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, indexes);
            metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, indexes);
            LOGGER.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas,
                    partitions);
        }

        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        maybeUpdateClusterBlock(indexes, blocks, IndexMetadata.INDEX_READ_ONLY_BLOCK,
                IndexMetadata.INDEX_READ_ONLY_SETTING, openSettings);
        maybeUpdateClusterBlock(indexes, blocks, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING, openSettings);
        maybeUpdateClusterBlock(indexes, blocks, IndexMetadata.INDEX_METADATA_BLOCK,
                IndexMetadata.INDEX_BLOCKS_METADATA_SETTING, openSettings);
        maybeUpdateClusterBlock(indexes, blocks, IndexMetadata.INDEX_WRITE_BLOCK,
                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING, openSettings);
        maybeUpdateClusterBlock(indexes, blocks, IndexMetadata.INDEX_READ_BLOCK,
                IndexMetadata.INDEX_BLOCKS_READ_SETTING, openSettings);

        for (IndexMetadata indexMetadata : indexes) {
            Settings.Builder updates = Settings.builder();
            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
            Settings toUpdate = indexMetadata.getState() == IndexMetadata.State.OPEN ? openSettings : closedSettings;
            String indexName = indexMetadata.getIndex().getName();
            // TODO can we improve updateDynamicSettings so that it returns false if the update
            // doesn't actually change the value of the setting?  Then we can remove the `same` call
            boolean updated = indexMetadata.getState() == IndexMetadata.State.OPEN ?
                indexScopedSettings.updateDynamicSettings(toUpdate, indexSettings, updates, indexName) :
                indexScopedSettings.updateSettings(toUpdate, indexSettings, updates, indexName);
            if (updated) {
                Settings finalSettings = indexSettings.build();
                indexScopedSettings.validate(
                        finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                if (same(indexMetadata.getSettings(), finalSettings) == false) {
                    metadataBuilder.put(
                        IndexMetadata.builder(indexMetadata).settings(finalSettings).incrementSettingsVersion());
                }
            }

        }

        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder)
                .routingTable(routingTableBuilder.build()).blocks(blocks).build();

        // now, reroute in case things change that require it (like number of replicas)
        updatedState = allocationService.reroute(updatedState, "settings update");
        try {
            for (IndexMetadata indexMetadata : indexes) {
                final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(indexMetadata.getIndex());
                indicesService.verifyIndexMetadata(indexMetadata, updatedMetadata);
                if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    // Now check that we can create the index with the updated settings (dynamic and
                    // non-dynamic).
                    // This step is mandatory since we allow to update non-dynamic settings on
                    // closed indices.
                    indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return updatedState;
    }

    public static int getTotalNewShards(IndexMetadata indexMetadata, int updatedNumberOfReplicas) {
        int shardsInIndex = indexMetadata.getNumberOfShards();
        int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    private static void maybeUpdateClusterBlock(List<IndexMetadata> indexes, ClusterBlocks.Builder blocks, ClusterBlock block, Setting<Boolean> setting, Settings openSettings) {
        if (setting.exists(openSettings)) {
            final boolean updateBlock = setting.get(openSettings);
            for (IndexMetadata im : indexes) {
                String indexUUID = im.getIndex().getUUID();
                if (updateBlock) {
                    blocks.addIndexBlock(indexUUID, block);
                } else {
                    blocks.removeIndexBlock(indexUUID, block);
                }
            }
        }
    }
}
