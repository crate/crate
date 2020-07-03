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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.IndexSettings.same;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetadataUpdateSettingsService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataUpdateSettingsService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public MetadataUpdateSettingsService(ClusterService clusterService, AllocationService allocationService,
                                         IndexScopedSettings indexScopedSettings, IndicesService indicesService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
    }

    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final Settings normalizedSettings = Settings.builder().put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(
                normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false), // don't validate wildcards
                false, // don't validate dependencies here we check it below never allow to change the number of shards
                true); // validate internal or private index settings
        for (String key : normalizedSettings.keySet()) {
            Setting setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " + normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        clusterService.submitStateUpdateTask(
            "update-settings",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(
                Priority.URGENT,
                request,
                listener) {

                    @Override
                    protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                        return new ClusterStateUpdateResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {

                        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                        // allow to change any settings to a close index, and only allow dynamic settings to be changed
                        // on an open index
                        Set<Index> openIndices = new HashSet<>();
                        Set<Index> closeIndices = new HashSet<>();
                        final String[] actualIndices = new String[request.indices().length];
                        for (int i = 0; i < request.indices().length; i++) {
                            Index index = request.indices()[i];
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

                        int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1);
                        if (updatedNumberOfReplicas != -1 && preserveExisting == false) {

                            // we do *not* update the in sync allocation ids as they will be removed upon the first index
                            // operation which make these copies stale
                            // TODO: update the list once the data is deleted by the node?
                            routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                            metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                            LOGGER.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                        }

                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_ONLY_BLOCK, IndexMetadata.INDEX_READ_ONLY_SETTING, openSettings);
                        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK, IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING, openSettings);
                        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_METADATA_BLOCK, IndexMetadata.INDEX_BLOCKS_METADATA_SETTING, openSettings);
                        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_WRITE_BLOCK, IndexMetadata.INDEX_BLOCKS_WRITE_SETTING, openSettings);
                        maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_BLOCK, IndexMetadata.INDEX_BLOCKS_READ_SETTING, openSettings);

                        if (!openIndices.isEmpty()) {
                            for (Index index : openIndices) {
                                IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                                Settings.Builder updates = Settings.builder();
                                Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                                if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                                    if (preserveExisting) {
                                        indexSettings.put(indexMetadata.getSettings());
                                    }
                                    Settings finalSettings = indexSettings.build();
                                    indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
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
                                    if (preserveExisting) {
                                        indexSettings.put(indexMetadata.getSettings());
                                    }
                                    Settings finalSettings = indexSettings.build();
                                    indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                                    metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                                }
                            }
                        }

                        // increment settings versions
                        for (final String index : actualIndices) {
                            if (same(currentState.metadata().index(index).getSettings(), metadataBuilder.get(index).getSettings()) == false) {
                                final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                                builder.settingsVersion(1 + builder.settingsVersion());
                                metadataBuilder.put(builder);
                            }
                        }

                        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).blocks(blocks).build();

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
                            throw ExceptionsHelper.convertToElastic(ex);
                        }
                        return updatedState;
                    }
                }
        );
    }

    private int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
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


    public void upgradeIndexSettings(final UpgradeSettingsClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
            "update-index-compatibility-versions",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(
                Priority.URGENT,
                request,
                listener
            ) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (Map.Entry<String, Tuple<Version, String>> entry : request.versions().entrySet()) {
                        String index = entry.getKey();
                        IndexMetadata indexMetadata = metadataBuilder.get(index);
                        if (indexMetadata != null) {
                            if (Version.CURRENT.equals(indexMetadata.getCreationVersion()) == false) {
                                // no reason to pollute the settings, we didn't really upgrade anything
                                metadataBuilder.put(
                                        IndexMetadata
                                        .builder(indexMetadata)
                                        .settings(
                                            Settings
                                            .builder()
                                            .put(indexMetadata.getSettings())
                                            .put(IndexMetadata.SETTING_VERSION_UPGRADED, entry.getValue().v1()))
                                        .settingsVersion(1 + indexMetadata.getSettingsVersion())
                                );
                            }
                        }
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }
            }
        );
    }
}
