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

package org.elasticsearch.snapshots;

import static io.crate.analyze.SnapshotSettings.SCHEMA_RENAME_PATTERN;
import static io.crate.analyze.SnapshotSettings.SCHEMA_RENAME_REPLACEMENT;
import static io.crate.analyze.SnapshotSettings.TABLE_RENAME_PATTERN;
import static io.crate.analyze.SnapshotSettings.TABLE_RENAME_REPLACEMENT;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED;
import static org.elasticsearch.snapshots.SnapshotUtils.filterIndices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest.TableOrPartition;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.exceptions.Exceptions;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.PartitionAlreadyExistsException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.execution.ddl.Templates;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreRequest, List, org.elasticsearch.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using {@link RoutingTable.Builder#addAsRestore(IndexMetadata, SnapshotRecoverySource)}
 * method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link ShardRouting#recoverySource()} property.
 * <p>
 * At the end of the successful restore process {@code RestoreService} calls {@link #cleanupRestoreState(ClusterChangedEvent)},
 * which removes {@link RestoreInProgress} when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 */
public class RestoreService implements ClusterStateApplier {

    private static final Logger LOGGER = LogManager.getLogger(RestoreService.class);

    private static final Set<String> UNMODIFIABLE_SETTINGS = Set.of(
            SETTING_NUMBER_OF_SHARDS,
            SETTING_VERSION_CREATED,
            SETTING_INDEX_UUID,
            SETTING_CREATION_DATE,
            IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(),
            SETTING_HISTORY_UUID);

    // It's OK to change some settings, but we shouldn't allow simply removing them
    private static final Set<String> UNREMOVABLE_SETTINGS;

    static {
        Set<String> unremovable = new HashSet<>(UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(AutoExpandReplicas.SETTING_KEY);
        unremovable.add(SETTING_VERSION_UPGRADED);
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final AllocationService allocationService;

    private final MetadataCreateIndexService createIndexService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ClusterSettings clusterSettings;

    private final CleanRestoreStateTaskExecutor cleanRestoreStateTaskExecutor;

    private final ShardLimitValidator shardLimitValidator;

    private final MetadataUpgrader metadataUpgrader;

    public RestoreService(ClusterService clusterService,
                          RepositoriesService repositoriesService,
                          AllocationService allocationService,
                          MetadataCreateIndexService createIndexService,
                          MetadataIndexUpgradeService metadataIndexUpgradeService,
                          MetadataUpgrader metadataUpgrader,
                          ClusterSettings clusterSettings,
                          ShardLimitValidator shardLimitValidator) {
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.metadataUpgrader = metadataUpgrader;
        if (DiscoveryNode.isMasterEligibleNode(clusterService.getSettings())) {
            clusterService.addStateApplier(this);
        }
        this.clusterSettings = clusterSettings;
        this.cleanRestoreStateTaskExecutor = new CleanRestoreStateTaskExecutor(LOGGER);
        this.shardLimitValidator = shardLimitValidator;
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param tablesToRestore contains tables to restore when used in CREATE SNAPSHOT
     *                        and NULL when used in logical replication.
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreRequest request,
                                @Nullable final List<TableOrPartition> tablesToRestore,
                                final ActionListener<RestoreCompletionResponse> listener) {
        final String repositoryName = request.repositoryName;
        Repository repository;
        try {
            // Read snapshot info and metadata from the repository
            repository = repositoriesService.repository(repositoryName);
        } catch (Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", request.repositoryName + ":" + request.snapshotName), e);
            listener.onFailure(e);
            return;
        }
        repository.getRepositoryData().thenCompose(repositoryData -> {
            final String snapshotName = request.snapshotName;
            final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds().stream()
                .filter(s -> snapshotName.equals(s.getName())).findFirst();
            if (matchingSnapshotId.isPresent() == false) {
                throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
            }

            final SnapshotId snapshotId = matchingSnapshotId.get();
            return repository.getSnapshotInfo(snapshotId).thenCompose(snapshotInfo -> {
                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                // Make sure that we can restore from this snapshot
                validateSnapshotRestorable(repositoryName, snapshotInfo);

                // Resolve the indices from the snapshot that need to be restored

                List<String> resolvedIndices = new ArrayList<>();
                List<String> resolvedTemplates = new ArrayList<>();

                resolveIndices(
                    request,
                    tablesToRestore,
                    snapshotInfo.indices(),
                    resolvedIndices,
                    resolvedTemplates
                );

                boolean includeIndices = request.includeIndices();
                if (includeIndices) {
                    // Empty list is resolved to "all indices" and we don't want break this behavior since RestoreService is used
                    // in other components (index recovery, logical replication).
                    // However, when restoring an empty partitioned table, there are no resolved indices
                    // but this should not be treated as "select all".
                    // In this case we force ignoring indices.
                    // See https://github.com/crate/crate/issues/14144
                    if (resolvedIndices.isEmpty() && tablesToRestore != null && tablesToRestore.size() > 0) {
                        includeIndices = false;
                    }
                }

                final List<String> indicesInSnapshot = includeIndices
                    ? filterIndices(snapshotInfo.indices(), resolvedIndices, request.indicesOptions())
                    : List.of();


                CompletableFuture<Metadata> futureGlobalMetadata;
                if (request.includeCustomMetadata()
                    || request.includeGlobalSettings()
                    || allTemplates(resolvedTemplates)
                    || (resolvedTemplates.isEmpty() == false)) {
                    futureGlobalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                } else {
                    futureGlobalMetadata = CompletableFuture.completedFuture(Metadata.EMPTY_METADATA);
                }
                return futureGlobalMetadata.thenCompose(globalMetadata -> {
                    var metadataBuilder = Metadata.builder(globalMetadata);
                    var indexIdsInSnapshot = repositoryData.resolveIndices(indicesInSnapshot);
                    var futureSnapshotIndexMetadata = repository.getSnapshotIndexMetadata(repositoryData, snapshotId, indexIdsInSnapshot);
                    return futureSnapshotIndexMetadata.thenAccept(snapshotIndexMetadata -> {
                        for (IndexMetadata indexMetadata : snapshotIndexMetadata) {
                            metadataBuilder.put(indexMetadata, false);
                        }
                        final Metadata metadata = metadataBuilder.build();
                        // Apply renaming on index names, returning a map of names where
                        // the key is the renamed index and the value is the original name
                        final Map<String, String> indices = applyRenameToIndices(request, indicesInSnapshot);

                        // Now we can start the actual restore process by adding shards to be recovered in the cluster state
                        // and updating cluster metadata (global and index) as needed
                        RestoreSnapshotUpdateTask updateTask = new RestoreSnapshotUpdateTask(
                            snapshotInfo,
                            snapshotId,
                            repositoryData,
                            snapshot,
                            listener,
                            request,
                            indices,
                            resolvedTemplates,
                            metadata
                        );
                        clusterService.submitStateUpdateTask("restore_snapshot[" + snapshotName + ']', updateTask);
                    });
                });
            });
        }).exceptionally(t -> {
            listener.onFailure(Exceptions.toException(t));
            return null;
        });
    }

    private boolean allTemplates(Collection<String> resolvedTemplates) {
        return resolvedTemplates.size() == 1 && resolvedTemplates.contains(Metadata.ALL);
    }

    /**
    * Resolves indices and templates from the request.
    * @param resolvedIndices is used to accumulate all resolved indices (or empty list to indicate all indices).
    * @param resolvedTemplates is used to accumulate all resolved templates (or "_all" to indicate all templates).
    */
    @VisibleForTesting
    static void resolveIndices(RestoreRequest request,
                               @Nullable List<TableOrPartition> tablesToRestore,
                               List<String> availableIndices,
                               List<String> resolvedIndices,
                               List<String> resolvedTemplates) {
        if (tablesToRestore != null) {
            for (TableOrPartition tableOrPartition : tablesToRestore) {
                String partitionTemplate = PartitionName.templateName(
                    tableOrPartition.table().schema(),
                    tableOrPartition.table().name()
                );

                if (tableOrPartition.partitionIdent() != null) {
                    resolvedIndices.add(
                        IndexName.encode(tableOrPartition.table().schema(), tableOrPartition.table().name(), tableOrPartition.partitionIdent())
                    );
                    resolvedTemplates.add(partitionTemplate);
                } else if (request.indicesOptions().ignoreUnavailable()) {
                    // If ignoreUnavailable is true, it's cheaper to simply
                    // return indexName and the partitioned wildcard instead
                    // checking if it's a partitioned table or not
                    resolvedIndices.add(tableOrPartition.table().indexNameOrAlias());
                    // For the case its a partitioned table we restore all partitions and the templates
                    resolvedIndices.add(partitionTemplate + "*");
                    resolvedTemplates.add(partitionTemplate);
                } else {
                    String name = tableOrPartition.table().indexNameOrAlias();
                    boolean found = false;
                    for (String index : availableIndices) {
                        if (name.equals(index)) {
                            resolvedIndices.add(index);
                            found = true;
                            break;
                        } else if (isIndexPartitionOfTable(index, tableOrPartition.table())) {
                            // add a partitions wildcard
                            // to match all partitions if a partitioned table was meant
                            resolvedIndices.add(partitionTemplate + "*");
                            resolvedTemplates.add(partitionTemplate);
                            found = true;
                            break;
                        }
                    }
                    if (found == false) {
                        resolvedTemplates.add(partitionTemplate);
                    }
                }
            }
        } else {
            // Use request values, for BWC and for restore from logical replication.
            for (String index: request.indices()) {
                resolvedIndices.add(index);
            }
            for (String template: request.templates()) {
                resolvedTemplates.add(template);
            }
        }

        if (request.includeIndices() && resolvedTemplates.isEmpty()) {
            resolvedTemplates.add(Metadata.ALL);
        }
    }

    public static boolean isIndexPartitionOfTable(String index, RelationName relationName) {
        return IndexName.isPartitioned(index) &&
            PartitionName.fromIndexOrTemplate(index).relationName().equals(relationName);
    }

    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : oldRestore) {
            ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = null;
            for (ObjectObjectCursor<ShardId, ShardRestoreStatus> cursor : entry.shards()) {
                ShardId shardId = cursor.key;
                if (deletedIndices.contains(shardId.getIndex())) {
                    changesMade = true;
                    if (shardsBuilder == null) {
                        shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                    }
                    shardsBuilder.put(shardId, new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "index was deleted"));
                }
            }
            if (shardsBuilder != null) {
                ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), overallState(RestoreInProgress.State.STARTED, shards), entry.indices(), shards));
            } else {
                builder.add(entry);
            }
        }
        if (changesMade) {
            return builder.build();
        } else {
            return oldRestore;
        }
    }

    private final class RestoreSnapshotUpdateTask extends ClusterStateUpdateTask {

        private final SnapshotInfo snapshotInfo;
        private final SnapshotId snapshotId;
        private final RepositoryData repositoryData;
        private final Snapshot snapshot;
        private final ActionListener<RestoreCompletionResponse> listener;
        private final RestoreRequest request;
        private final Map<String, String> indices;
        private final List<String> templates;

        private final Metadata metadata;
        final String restoreUUID = UUIDs.randomBase64UUID();
        RestoreInfo restoreInfo = null;

        private RestoreSnapshotUpdateTask(SnapshotInfo snapshotInfo,
                                          SnapshotId snapshotId,
                                          RepositoryData repositoryData,
                                          Snapshot snapshot,
                                          ActionListener<RestoreCompletionResponse> listener,
                                          RestoreRequest request,
                                          Map<String, String> indices,
                                          List<String> templates,
                                          Metadata metadata) {
            this.snapshotInfo = snapshotInfo;
            this.snapshotId = snapshotId;
            this.repositoryData = repositoryData;
            this.snapshot = snapshot;
            this.listener = listener;
            this.request = request;
            this.indices = indices;
            this.templates = templates;
            this.metadata = metadata;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            // Check if the snapshot to restore is currently being deleted
            SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY);
            if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshotId))) {
                throw new ConcurrentSnapshotExecutionException(snapshot, "cannot restore a snapshot while a snapshot deletion is in-progress [" +
                    deletionsInProgress.getEntries().get(0) + "]");
            }
            // Updating cluster state
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
            ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
            ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards;
            Set<String> aliases = new HashSet<>();

            if (indices.isEmpty() == false) {
                // We have some indices to restore
                ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder =
                    ImmutableOpenMap.builder();
                final Version minIndexCompatibilityVersion = currentState.nodes().getMaxNodeVersion()
                    .minimumIndexCompatibilityVersion();
                for (Map.Entry<String, String> indexEntry : indices.entrySet()) {
                    String index = indexEntry.getValue();
                    boolean partial = checkPartial(index);
                    SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(restoreUUID, snapshot,
                        snapshotInfo.version(), repositoryData.resolveIndexId(index));
                    IndexMetadata snapshotIndexMetadata = metadata.index(index);
                    if (snapshotIndexMetadata == null) {
                        throw new IndexNotFoundException(
                            "Failed to find index '" + index + "' at the metadata of the current snapshot '" + snapshot + "'",
                            index
                        );
                    }
                    snapshotIndexMetadata = updateIndexSettings(snapshotIndexMetadata,
                                                                request.indexSettings(), request.ignoreIndexSettings());
                    try {
                        String indexName = snapshotIndexMetadata.getIndex().getName();
                        snapshotIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(
                            snapshotIndexMetadata,
                            IndexName.isPartitioned(indexName) ?
                                currentState.metadata().templates().get(PartitionName.templateName(indexName)) :
                                null,
                            minIndexCompatibilityVersion);
                    } catch (Exception ex) {
                        throw new SnapshotRestoreException(snapshot, "cannot restore index [" + index +
                                                                        "] because it cannot be upgraded", ex);
                    }
                    // Check that the index is closed or doesn't exist
                    String renamedIndexName = indexEntry.getKey();
                    IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                    IntSet ignoreShards = new IntHashSet();
                    final Index renamedIndex;
                    if (currentIndexMetadata == null) {
                        // Index doesn't exist - create it and start recovery
                        // Make sure that the index we are about to create has a validate name
                        IndexName.validate(renamedIndexName);
                        createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);
                        IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                            .state(IndexMetadata.State.OPEN)
                            .index(renamedIndexName);

                        if (request.hasNonDefaultRenamePatterns() && snapshotIndexMetadata.getAliases().isEmpty() == false) {
                            // Partitioned tables are created with an alias.
                            // A new index, created on top of an existing index with a different name,
                            // must use renamed alias instead of the copied one to avoid restoring into the source tables instead of renamed ones.
                            // Regular tables don't have AliasMetadata, so we reflect that for renamed indices.
                            var renamedAlias = RelationName.fromIndexName(renamedIndexName).indexNameOrAlias();
                            indexMdBuilder.removeAllAliases();
                            indexMdBuilder.putAlias(new AliasMetadata(renamedAlias));
                        }

                        Builder indexSettingsBuilder = Settings.builder()
                            .put(snapshotIndexMetadata.getSettings())
                            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                        if (snapshotIndexMetadata.getCreationVersion().onOrAfter(Version.V_5_1_0)
                                || currentState.nodes().getMinNodeVersion().onOrAfter(Version.V_5_1_0)) {
                            indexSettingsBuilder.put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID());
                        }
                        indexMdBuilder.settings(indexSettingsBuilder);

                        shardLimitValidator.validateShardLimit(snapshotIndexMetadata.getSettings(), currentState);
                        if (!request.includeAliases() && !snapshotIndexMetadata.getAliases().isEmpty()) {
                            // Remove all aliases - they shouldn't be restored
                            indexMdBuilder.removeAllAliases();
                        } else {
                            for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                aliases.add(alias.value);
                            }
                        }
                        IndexMetadata updatedIndexMetadata = indexMdBuilder.build();
                        if (partial) {
                            populateIgnoredShards(index, ignoreShards);
                        }
                        rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                        blocks.addBlocks(updatedIndexMetadata);
                        mdBuilder.put(updatedIndexMetadata, true);
                        renamedIndex = updatedIndexMetadata.getIndex();
                    } else {
                        validateExistingIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);
                        // Index exists and it's closed - open it in metadata and start recovery
                        IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata).state(IndexMetadata.State.OPEN);
                        indexMdBuilder.version(Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion()));
                        indexMdBuilder.mappingVersion(Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion()));
                        indexMdBuilder.settingsVersion(Math.max(snapshotIndexMetadata.getSettingsVersion(), 1 + currentIndexMetadata.getSettingsVersion()));

                        for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                            indexMdBuilder.primaryTerm(shard, Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard)));
                        }

                        if (!request.includeAliases()) {
                            // Remove all snapshot aliases
                            if (!snapshotIndexMetadata.getAliases().isEmpty()) {
                                indexMdBuilder.removeAllAliases();
                            }
                            // Add existing aliases
                            for (ObjectCursor<AliasMetadata> alias : currentIndexMetadata.getAliases().values()) {
                                indexMdBuilder.putAlias(alias.value);
                            }
                        } else {
                            for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                aliases.add(alias.value);
                            }
                        }
                        indexMdBuilder.settings(Settings.builder()
                                                    .put(snapshotIndexMetadata.getSettings())
                                                    .put(IndexMetadata.SETTING_INDEX_UUID, currentIndexMetadata.getIndexUUID()));
                        IndexMetadata updatedIndexMetadata = indexMdBuilder.index(renamedIndexName).build();
                        rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                        blocks.updateBlocks(updatedIndexMetadata);
                        mdBuilder.put(updatedIndexMetadata, true);
                        renamedIndex = updatedIndexMetadata.getIndex();
                    }
                    for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                        if (!ignoreShards.contains(shard)) {
                            shardsBuilder.put(new ShardId(renamedIndex, shard),
                                                new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
                        } else {
                            shardsBuilder.put(new ShardId(renamedIndex, shard),
                                                new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId(), RestoreInProgress.State.FAILURE));
                        }
                    }
                }

                shards = shardsBuilder.build();
                RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(
                    restoreUUID,
                    snapshot,
                    overallState(RestoreInProgress.State.INIT, shards),
                    List.copyOf(indices.keySet()),
                    shards
                );

                builder.putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder(
                    currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(restoreEntry).build());
            } else {
                shards = ImmutableOpenMap.of();
            }

            validateExistingTemplates(templates);
            checkAliasNameConflicts(indices, aliases);
            // Restore templates (but do NOT overwrite existing templates)
            restoreTemplates(templates, mdBuilder, currentState);

            // Restore global state if needed
            if (request.includeGlobalSettings() && metadata.persistentSettings() != null) {
                Settings settings = metadata.persistentSettings();

                // CrateDB patch to only restore defined settings
                if (request.globalSettings().length > 0) {
                    var filteredSettingBuilder = Settings.builder();
                    for (String prefix : request.globalSettings()) {
                        filteredSettingBuilder.put(settings.filter(s -> s.startsWith(prefix)));
                    }
                    settings = filteredSettingBuilder.build();
                }

                clusterSettings.validateUpdate(settings);
                mdBuilder.persistentSettings(settings);
            }

            if (request.includeCustomMetadata() && metadata.customs() != null) {
                // CrateDB patch to only restore defined custom metadata types
                List<String> customMetadataTypes = Arrays.asList(request.customMetadataTypes());
                boolean includeAll = customMetadataTypes.size() == 0;

                for (ObjectObjectCursor<String, Metadata.Custom> cursor : metadata.customs()) {
                    if (!RepositoriesMetadata.TYPE.equals(cursor.key)) {
                        // Don't restore repositories while we are working with them
                        // TODO: Should we restore them at the end?

                        if (includeAll || customMetadataTypes.contains(cursor.key)) {
                            mdBuilder.putCustom(cursor.key, cursor.value);
                        }
                    }
                }
            }

            if (completed(shards)) {
                // We don't have any indices to restore - we are done
                restoreInfo = new RestoreInfo(snapshotId.getName(), Collections.unmodifiableList(new ArrayList<>(indices.keySet())), shards.size(), shards.size() - failedShards(shards));
            }

            RoutingTable rt = rtBuilder.build();
            ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
            return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
        }

        private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
            for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                if (aliases.contains(renamedIndex.getKey())) {
                    throw new SnapshotRestoreException(snapshot, "cannot rename index [" + renamedIndex.getValue() +
                                                                    "] into [" + renamedIndex.getKey() + "] because of conflict with an alias with the same name");
                }
            }
        }

        private void populateIgnoredShards(String index, IntSet ignoreShards) {
            for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
                if (index.equals(failure.index())) {
                    ignoreShards.add(failure.shardId());
                }
            }
        }

        private boolean checkPartial(String index) {
            // Make sure that index was fully snapshotted
            if (failed(snapshotInfo, index)) {
                if (request.partial()) {
                    return true;
                } else {
                    throw new SnapshotRestoreException(snapshot, "index [" + index + "] wasn't fully snapshotted - cannot " + "restore");
                }
            } else {
                return false;
            }
        }

        private void validateExistingIndex(
            IndexMetadata currentIndexMetadata,
            IndexMetadata snapshotIndexMetadata,
            String renamedIndex,
            boolean partial) {
            // Index exist - checking that it's closed
            if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                // TODO: Enable restore for open indices
                IndexParts indexParts = IndexName.decode(renamedIndex);
                RelationName relationName = new RelationName(indexParts.schema(), indexParts.table());
                if (indexParts.isPartitioned()) {
                    throw new PartitionAlreadyExistsException(new PartitionName(relationName, indexParts.partitionIdent()));
                } else {
                    throw new RelationAlreadyExists(relationName);
                }
            }
            // Index exist - checking if it's partial restore
            if (partial) {
                throw new SnapshotRestoreException(snapshot, "cannot restore partial index [" + renamedIndex + "] because such index already exists");
            }
            // Make sure that the number of shards is the same. That's the only thing that we cannot change
            if (currentIndexMetadata.getNumberOfShards() != snapshotIndexMetadata.getNumberOfShards()) {
                throw new SnapshotRestoreException(snapshot, "cannot restore index [" + renamedIndex + "] with [" + currentIndexMetadata.getNumberOfShards() +
                                                                "] shards from a snapshot of index [" + snapshotIndexMetadata.getIndex().getName() + "] with [" +
                                                                snapshotIndexMetadata.getNumberOfShards() + "] shards");
            }
        }

        /**
            * Optionally updates index settings in indexMetadata by removing settings listed in ignoreSettings and
            * merging them with settings in changeSettings.
            */
        private IndexMetadata updateIndexSettings(IndexMetadata indexMetadata,
                                                    Settings changeSettings,
                                                    String[] ignoreSettings) {
            if (changeSettings.names().isEmpty() && ignoreSettings.length == 0) {
                return indexMetadata;
            }
            Settings normalizedChangeSettings = Settings.builder().put(changeSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
            IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
            Settings settings = indexMetadata.getSettings();
            Set<String> keyFilters = new HashSet<>();
            List<String> simpleMatchPatterns = new ArrayList<>();
            for (String ignoredSetting : ignoreSettings) {
                if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                    if (UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                        throw new SnapshotRestoreException(snapshot, "cannot remove setting [" + ignoredSetting + "] on restore");
                    } else {
                        keyFilters.add(ignoredSetting);
                    }
                } else {
                    simpleMatchPatterns.add(ignoredSetting);
                }
            }

            Predicate<String> settingsFilter = k -> {
                if (UNREMOVABLE_SETTINGS.contains(k) == false) {
                    for (String filterKey : keyFilters) {
                        if (k.equals(filterKey)) {
                            return false;
                        }
                    }
                    for (String pattern : simpleMatchPatterns) {
                        if (Regex.simpleMatch(pattern, k)) {
                            return false;
                        }
                    }
                }
                return true;
            };

            Settings.Builder settingsBuilder = Settings.builder()
                .put(settings.filter(settingsFilter))
                .put(normalizedChangeSettings.filter(k -> {
                    if (UNMODIFIABLE_SETTINGS.contains(k)) {
                        throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
                    } else {
                        return true;
                    }
                }));
            settingsBuilder.remove(IndexMetadata.VERIFIED_BEFORE_CLOSE_SETTING.getKey());
            return builder.settings(settingsBuilder).build();
        }

        private void restoreTemplates(List<String> templates,
                                      Metadata.Builder mdBuilder,
                                      ClusterState currentState) {
            ImmutableOpenMap<String, IndexTemplateMetadata> templateMetadata = metadata.templates();
            if (templateMetadata == null) {
                return;
            }
            Map<String, IndexTemplateMetadata> includedTemplates = new HashMap<>();
            boolean allTemplates = allTemplates(templates);
            boolean applyRenamePattern = request.hasNonDefaultRenamePatterns();
            for (var cursor : templateMetadata) {
                String templateName = cursor.key;
                IndexTemplateMetadata indexTemplateMetadata = cursor.value;
                if (allTemplates || templates.contains(templateName)) {
                    IndexTemplateMetadata previous;
                    if (applyRenamePattern) {
                        IndexParts indexParts = IndexName.decode(templateName);
                        String schema = indexParts.schema();
                        String table = indexParts.table();
                        RelationName newName = new RelationName(
                            schema.replaceAll(request.schemaRenamePattern(), request.schemaRenameReplacement()),
                            table.replaceAll(request.tableRenamePattern(), request.tableRenameReplacement())
                        );
                        IndexTemplateMetadata renamedIndexTemplateMetadata = Templates.withName(indexTemplateMetadata, newName).build();
                        previous = includedTemplates.put(newName.indexNameOrAlias(), renamedIndexTemplateMetadata);
                    } else {
                        previous = includedTemplates.put(templateName, indexTemplateMetadata);
                    }
                    if (previous != null) {
                        throw new SnapshotRestoreException(
                            request.repositoryName,
                            request.snapshotName,
                            String.format(
                                Locale.ENGLISH,
                                "Rename conflict for partitioned table `%s`. `%s` already exists. Cannot rename `%s` to the same name",
                                IndexName.decode(templateName).toRelationName().fqn(),
                                templateName,
                                previous.name()
                            )
                        );
                    }
                }
            }
            includedTemplates = metadataUpgrader.indexTemplateMetadataUpgraders.apply(includedTemplates);
            for (var indexTemplateMetadata : includedTemplates.values()) {
                mdBuilder.put(indexTemplateMetadata);
            }
        }

        private void validateExistingTemplates(List<String> templates) {
            if (request.indicesOptions().ignoreUnavailable() || allTemplates(templates)) {
                return;
            }
            for (String template : templates) {
                if (!metadata.templates().containsKey(template)) {
                    throw new ResourceNotFoundException("[{}] template not found", template);
                }
            }
        }

        @Override
        public void onFailure(String source, Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshotId), e);
            listener.onFailure(e);
        }

        @Override
        public TimeValue timeout() {
            return request.masterNodeTimeout();
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            listener.onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
        }
    }

    public static final class RestoreCompletionResponse {
        private final String uuid;
        private final Snapshot snapshot;
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(final String uuid, final Snapshot snapshot, final RestoreInfo restoreInfo) {
            this.uuid = uuid;
            this.snapshot = snapshot;
            this.restoreInfo = restoreInfo;
        }

        public String getUuid() {
            return uuid;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
        private final Map<String, Updates> shardChanges = new HashMap<>();

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    changes(recoverySource).shards.put(
                        initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS));
                }
            }
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            if (failedShard.primary() && failedShard.initializing()) {
                RecoverySource recoverySource = failedShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    // mark restore entry for this shard as failed when it's due to a file corruption. There is no need wait on retries
                    // to restore this shard on another node if the snapshot files are corrupt. In case where a node just left or crashed,
                    // however, we only want to acknowledge the restore operation once it has been successfully restored on another node.
                    if (unassignedInfo.getFailure() != null && Lucene.isCorruptionException(unassignedInfo.getFailure().getCause())) {
                        changes(recoverySource).shards.put(
                            failedShard.shardId(), new ShardRestoreStatus(failedShard.currentNodeId(),
                                RestoreInProgress.State.FAILURE, unassignedInfo.getFailure().getCause().getMessage()));
                    }
                }
            }
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            // if we force an empty primary, we should also fail the restore entry
            if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT &&
                initializedShard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                changes(unassignedShard.recoverySource()).shards.put(
                    unassignedShard.shardId(),
                    new ShardRestoreStatus(null,
                        RestoreInProgress.State.FAILURE, "recovery source type changed from snapshot to " + initializedShard.recoverySource())
                );
            }
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                if (newUnassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    String reason = "shard could not be allocated to any of the nodes";
                    changes(recoverySource).shards.put(
                        unassignedShard.shardId(),
                        new ShardRestoreStatus(unassignedShard.currentNodeId(), RestoreInProgress.State.FAILURE, reason));
                }
            }
        }

        /**
         * Helper method that creates update entry for the given recovery source's restore uuid
         * if such an entry does not exist yet.
         */
        private Updates changes(RecoverySource recoverySource) {
            assert recoverySource.getType() == RecoverySource.Type.SNAPSHOT;
            return shardChanges.computeIfAbsent(((SnapshotRecoverySource) recoverySource).restoreUUID(), k -> new Updates());
        }

        private static class Updates {
            private Map<ShardId, ShardRestoreStatus> shards = new HashMap<>();
        }

        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
                for (RestoreInProgress.Entry entry : oldRestore) {
                    Updates updates = shardChanges.get(entry.uuid());
                    ImmutableOpenMap<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.shards.isEmpty() == false) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder(shardStates);
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.shards.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards));
                    } else {
                        builder.add(entry);
                    }
                }
                return builder.build();
            } else {
                return oldRestore;
            }
        }
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, String restoreUUID) {
        return state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).get(restoreUUID);
    }

    static class CleanRestoreStateTaskExecutor implements ClusterStateTaskExecutor<CleanRestoreStateTaskExecutor.Task>, ClusterStateTaskListener {

        static class Task {
            final String uuid;

            Task(String uuid) {
                this.uuid = uuid;
            }

            @Override
            public String toString() {
                return "clean restore state for restore " + uuid;
            }
        }

        private final Logger logger;

        CleanRestoreStateTaskExecutor(Logger logger) {
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            Set<String> completedRestores = tasks.stream().map(e -> e.uuid).collect(Collectors.toSet());
            RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
            boolean changed = false;
            for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
                if (completedRestores.contains(entry.uuid())) {
                    changed = true;
                } else {
                    restoreInProgressBuilder.add(entry);
                }
            }
            if (changed == false) {
                return resultBuilder.build(currentState);
            }
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(currentState.customs());
            builder.put(RestoreInProgress.TYPE, restoreInProgressBuilder.build());
            ImmutableOpenMap<String, ClusterState.Custom> customs = builder.build();
            return resultBuilder.build(ClusterState.builder(currentState).customs(customs).build());
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing restore state update [{}]", source);
        }

    }

    private void cleanupRestoreState(ClusterChangedEvent event) {
        for (RestoreInProgress.Entry entry : event.state().custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (entry.state().completed()) {
                assert completed(entry.shards()) : "state says completed but restore entries are not";
                clusterService.submitStateUpdateTask(
                    "clean up snapshot restore state",
                    new CleanRestoreStateTaskExecutor.Task(entry.uuid()),
                    ClusterStateTaskConfig.build(Priority.URGENT),
                    cleanRestoreStateTaskExecutor,
                    cleanRestoreStateTaskExecutor);
            }
        }
    }

    public static RestoreInProgress.State overallState(RestoreInProgress.State nonCompletedState,
                                                       ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        boolean hasFailed = false;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return nonCompletedState;
            }
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        if (hasFailed) {
            return RestoreInProgress.State.FAILURE;
        } else {
            return RestoreInProgress.State.SUCCESS;
        }
    }

    public static boolean completed(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private Map<String, String> applyRenameToIndices(RestoreRequest request, List<String> indices) {

        Map<String, String> renamedIndices = new HashMap<>();
        boolean applyRenamePattern = request.hasNonDefaultRenamePatterns();
        for (String index : indices) {
            String renamed = index;
            // At least one non-default value is provided.
            if (applyRenamePattern) {
                IndexParts indexParts = IndexName.decode(renamed);
                String schema = indexParts.schema();
                String table = indexParts.table();
                table = table.replaceAll(request.tableRenamePattern(), request.tableRenameReplacement());
                schema = schema.replaceAll(request.schemaRenamePattern(), request.schemaRenameReplacement());

                // Use intermediate RelationName to do some validations
                // and also handle blob/doc schemas for non-partitioned tables
                RelationName renamedIdent = new RelationName(schema, table);
                if (indexParts.isPartitioned()) {
                    renamed = IndexName.encode(renamedIdent.schema(), renamedIdent.name(), indexParts.partitionIdent());
                } else {
                    renamed = renamedIdent.indexNameOrAlias();
                }
            }
            String previous = renamedIndices.put(renamed, index);
            if (previous != null) {
                throw new SnapshotRestoreException(request.repositoryName, request.snapshotName,
                    "indices [" + index + "] and [" + previous + "] are renamed into the same index [" + renamed + "]");
            }
        }
        return Collections.unmodifiableMap(renamedIndices);
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     */
    private void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (!snapshotInfo.state().restorable()) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "unsupported snapshot state [" + snapshotInfo.state() + "]");
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "the snapshot was created with CrateDB version [" + snapshotInfo.version() +
                                                   "] which is higher than the version of this node [" + Version.CURRENT + "]");
        }
    }

    private boolean failed(SnapshotInfo snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the indices that are currently being restored and that are contained in the indices-to-check set.
     */
    public static Set<Index> restoringIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            for (ObjectObjectCursor<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards()) {
                Index index = shard.key.getIndex();
                if (indicesToCheck.contains(index)
                    && shard.value.state().completed() == false
                    && currentState.metadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                cleanupRestoreState(event);
            }
        } catch (Exception t) {
            LOGGER.warn("Failed to update restore state ", t);
        }
    }

    /**
     * Restore snapshot request
     */
    public static class RestoreRequest {

        private final String cause;

        private final String repositoryName;

        private final String snapshotName;

        private final String[] indices;

        private final String[] templates;

        private final String tableRenamePattern;

        private final String tableRenameReplacement;

        private final String schemaRenamePattern;

        private final String schemaRenameReplacement;

        private final IndicesOptions indicesOptions;

        private final Settings settings;

        private final TimeValue masterNodeTimeout;

        private final boolean partial;

        private final boolean includeAliases;

        private final Settings indexSettings;

        private final String[] ignoreIndexSettings;

        private final boolean includeIndices;

        private final boolean includeCustomMetadata;

        private final String[] customMetadataTypes;

        private final boolean includeGlobalSettings;

        private final String[] globalSettings;

        /**
         * Constructs new restore request
         *
         * @param repositoryName     repositoryName
         * @param snapshotName       snapshotName
         * @param indices            list of indices to restore
         * @param templates          list of indices to templates
         * @param indicesOptions     indices options
         * @param tableRenamePattern      pattern to rename indices
         * @param tableRenameReplacement  replacement for renamed indices
         * @param settings           repository specific restore settings
         * @param masterNodeTimeout  master node timeout
         * @param partial            allow partial restore
         * @param indexSettings      index settings that should be changed on restore
         * @param ignoreIndexSettings index settings that shouldn't be restored
         * @param cause              cause for restoring the snapshot
         * @param includeIndices     include any index on restore
         * @param includeCustomMetadata include custom metadata types
         * @param customMetadataTypes custom metadata types to restore
         * @param includeGlobalSettings include any cluster settings on restore
         * @param globalSettings     global settings to restore
         */
        public RestoreRequest(String repositoryName, String snapshotName, String[] indices, String[] templates, IndicesOptions indicesOptions,
                              String tableRenamePattern, String tableRenameReplacement,
                              String schemaRenamePattern, String schemaRenameReplacement,
                              Settings settings,
                              TimeValue masterNodeTimeout, boolean partial, boolean includeAliases,
                              Settings indexSettings, String[] ignoreIndexSettings, String cause,
                              boolean includeIndices,
                              boolean includeCustomMetadata,
                              String[] customMetadataTypes,
                              boolean includeGlobalSettings,
                              String[] globalSettings) {
            this.repositoryName = Objects.requireNonNull(repositoryName);
            this.snapshotName = Objects.requireNonNull(snapshotName);
            this.indices = indices;
            this.templates = templates;
            this.tableRenamePattern = tableRenamePattern;
            this.tableRenameReplacement = tableRenameReplacement;
            this.schemaRenamePattern = schemaRenamePattern;
            this.schemaRenameReplacement = schemaRenameReplacement;
            this.indicesOptions = indicesOptions;
            this.settings = settings;
            this.masterNodeTimeout = masterNodeTimeout;
            this.partial = partial;
            this.includeAliases = includeAliases;
            this.indexSettings = indexSettings;
            this.ignoreIndexSettings = ignoreIndexSettings;
            this.cause = cause;
            this.includeIndices = includeIndices;
            this.includeCustomMetadata = includeCustomMetadata;
            this.customMetadataTypes = customMetadataTypes;
            this.includeGlobalSettings = includeGlobalSettings;
            this.globalSettings = globalSettings;
        }

        /**
         * Returns restore operation cause
         *
         * @return restore operation cause
         */
        public String cause() {
            return cause;
        }

        /**
         * Returns repository name
         *
         * @return repository name
         */
        public String repositoryName() {
            return repositoryName;
        }

        /**
         * Returns snapshot name
         *
         * @return snapshot name
         */
        public String snapshotName() {
            return snapshotName;
        }

        /**
         * Return the list of indices to be restored
         *
         * @return the list of indices
         */
        public String[] indices() {
            return indices;
        }

        public String[] templates() {
            return templates;
        }

        /**
         * Returns indices option flags
         *
         * @return indices options flags
         */
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public String tableRenamePattern() {
            return tableRenamePattern;
        }

        public String tableRenameReplacement() {
            return tableRenameReplacement;
        }

        public String schemaRenamePattern() {
            return schemaRenamePattern;
        }

        public String schemaRenameReplacement() {
            return schemaRenameReplacement;
        }

        /**
         * Returns repository-specific restore settings
         *
         * @return restore settings
         */
        public Settings settings() {
            return settings;
        }

        /**
         * Returns true if incomplete indices will be restored
         *
         * @return partial indices restore flag
         */
        public boolean partial() {
            return partial;
        }

        /**
         * Returns true if aliases should be restore during this restore operation
         *
         * @return restore aliases state flag
         */
        public boolean includeAliases() {
            return includeAliases;
        }

        /**
         * Returns index settings that should be changed on restore
         *
         * @return restore aliases state flag
         */
        public Settings indexSettings() {
            return indexSettings;
        }

        /**
         * Returns index settings that that shouldn't be restored
         *
         * @return restore aliases state flag
         */
        public String[] ignoreIndexSettings() {
            return ignoreIndexSettings;
        }

        /**
         * Returns true if any index should be restored
         */
        public boolean includeIndices() {
            return includeIndices;
        }

        /**
         * Returns true if custom metadata should be restored
         */
        public boolean includeCustomMetadata() {
            return includeCustomMetadata;
        }

        /**
         * Returns custom metadata types that should be restored
         *
         * @return  List of custom metadata types
         */
        public String[] customMetadataTypes() {
            return customMetadataTypes;
        }

        /**
         * Returns true if global cluster settings should be restored
         */
        public boolean includeGlobalSettings() {
            return includeGlobalSettings;
        }

        /**
         * Returns global state setting prefixes to include during the restore operation

         * @return  List of setting prefix to include
         */
        public String[] globalSettings() {
            return globalSettings;
        }

        /**
         * Return master node timeout
         *
         * @return master node timeout
         */
        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

        public boolean hasNonDefaultRenamePatterns() {
            return !tableRenamePattern().equals(TABLE_RENAME_PATTERN.getDefault(Settings.EMPTY))
                || !tableRenameReplacement().equals(TABLE_RENAME_REPLACEMENT.getDefault(Settings.EMPTY))
                || !schemaRenamePattern().equals(SCHEMA_RENAME_PATTERN.getDefault(Settings.EMPTY))
                || !schemaRenameReplacement().equals(SCHEMA_RENAME_REPLACEMENT.getDefault(Settings.EMPTY));
        }
    }
}
