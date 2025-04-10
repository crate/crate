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

package org.elasticsearch.action.admin.indices.create;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.gateway.GatewayMetaState.applyUpgrader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.VisibleForTesting;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.crate.exceptions.RelationUnknown;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.metadata.IndexName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;


/**
 * Creates one or more partitions within one cluster-state-update-task
 * <p>
 * This is more or less a more optimized version of {@link MetadataCreateIndexService}
 * <p>
 * It also has some limitations:
 * <p>
 * - all indices must actually have the same name pattern (only the first index is used to figure out which templates to use),
 *   which must be the case for partitions anyway.
 * - and alias / mappings / etc. are not taken from the request
 */
public class TransportCreatePartitions extends TransportMasterNodeAction<CreatePartitionsRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "indices:admin/bulk_create";

        private Action() {
            super(NAME);
        }
    }

    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final ActiveShardsObserver activeShardsObserver;
    private final ShardLimitValidator shardLimitValidator;

    private final AtomicBoolean upgraded;
    private final MetadataUpgradeService metadataUpgradeService;
    private final DocTableInfoFactory docTableInfoFactory;

    @Inject
    public TransportCreatePartitions(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     IndicesService indicesService,
                                     AllocationService allocationService,
                                     MetadataUpgradeService metadataUpgradeService,
                                     ShardLimitValidator shardLimitValidator,
                                     NodeContext nodeContext) {
        super(ACTION.name(), transportService, clusterService, threadPool, CreatePartitionsRequest::new);
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.metadataUpgradeService = metadataUpgradeService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
        this.shardLimitValidator = shardLimitValidator;
        this.upgraded = new AtomicBoolean(false);
        this.docTableInfoFactory = new DocTableInfoFactory(nodeContext);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(final CreatePartitionsRequest request,
                                   final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) throws ElasticsearchException {
        createIndices(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                List<String> indexNames = indexNames(request.partitionNames(), state);
                activeShardsObserver.waitForActiveShards(indexNames.toArray(String[]::new), ActiveShardCount.DEFAULT, request.ackTimeout(),
                    shardsAcked -> {
                        if (!shardsAcked && logger.isInfoEnabled()) {
                            RelationName relationName = request.relationName();
                            RelationMetadata.Table table = state.metadata().getRelation(relationName);
                            assert table != null : "table should be present in the cluster state";

                            logger.info("[{}] Table partitions created, but the operation timed out while waiting for " +
                                         "enough shards to be started. Timeout={}, wait_for_active_shards={}. " +
                                         "Consider decreasing the 'number_of_shards' table setting (currently: {}) or adding nodes to the cluster.",
                                relationName, request.timeout(),
                                SETTING_WAIT_FOR_ACTIVE_SHARDS.get(table.settings()),
                                INDEX_NUMBER_OF_SHARDS_SETTING.get(table.settings()));
                        }
                        listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
                    }, listener::onFailure);
            } else {
                logger.warn("[{}] Table partitions created, but publishing new cluster state timed out. Timeout={}",
                    request.relationName(), request.timeout());
                listener.onResponse(new AcknowledgedResponse(false));
            }
        }, listener::onFailure));
    }

    /**
     * This code is more or less the same as the stuff in {@link MetadataCreateIndexService}
     * but optimized for bulk operation without separate mapping/alias/index settings.
     */
    public ClusterState executeCreateIndices(ClusterState currentState,
                                             RelationMetadata.Table table,
                                             DocTableInfo docTableInfo,
                                             CreatePartitionsRequest request) throws Exception {
        String removalReason = null;
        Index testIndex = null;
        try {
            if (request.partitionNames().isEmpty()) {
                return currentState;
            }
            List<PartitionName> partitions = request.partitionNames();
            validatePartitions(currentState, table, partitions);

            // We always have only 1 matching template per pattern/table.
            // All indices in the request are related to a concrete partitioned table and
            // they all match the same template. Thus, we can use any of them to find matching template.
            String firstIndex = partitions.get(0).asIndexName();

            final Metadata metadata = currentState.metadata();
            Metadata.Builder newMetadataBuilder = Metadata.builder(metadata);

            // TODO: REMOVE? templates are not used anymore, do we need to add logic to the upgrade-to-relations-logic?
            // After https://github.com/crate/crate/commit/c8edfff8fe7713b071841b9db1aea26e3f500245,
            // we upgrade templates only on node startup in GatewayMetaState or on logical replication or on a snapshot restore.
            // That is not enough to prevent old templates being applied on new nodes.

            // Say, we are running a rolling upgrade.
            // One node has been upgraded to VERSION.CURRENT and a master node, which is on old version < CURRENT
            // creates partitions/new indices with old metadata/removed settings.
            // All upgraded nodes will get incoming old template via IndicesClusterStateService
            // and keep it forever as they already accomplished upgrade-on-startup.

            // We upgrade on the fly as a trade-off to the pre-5.6.0 state
            // where we used to upgrade template on each cluster change and submit synchronous  request via PutTemplate API to the master node to apply it.
            // Each new node either never becomes a master and doesn't really need upgrade or eventually upgrades when it becomes a master.
            // Since upgrade can be expensive for bulk inserts, we do it only once (per active master node).
            // After re-election, a new master will run it again, but only once.
            if (upgraded.compareAndSet(false, true)) {
                upgradeTemplates(metadata, newMetadataBuilder);
            }

            // Use only first index to validate that index can be created.
            // All indices share same table settings so no need to repeat validation for each index.
            Settings commonIndexSettings = createCommonIndexSettings(currentState, table.settings());

            final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(commonIndexSettings);
            final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(commonIndexSettings);
            // Shard limit check has to take to account all new partitions.
            final int numShardsToCreate = numberOfShards * (1 + numberOfReplicas) * partitions.size();
            shardLimitValidator.checkShardLimit(numShardsToCreate, currentState)
                .ifPresent(err -> {
                    final ValidationException e = new ValidationException();
                    e.addValidationError(err);
                    throw e;
                });

            int numTargetShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(commonIndexSettings);
            final int routingNumShards;
            final Version indexVersionCreated = commonIndexSettings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null);

            if (IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(commonIndexSettings)) {
                routingNumShards = numTargetShards;
            } else {
                routingNumShards = MetadataCreateIndexService.calculateNumRoutingShards(
                    numTargetShards, indexVersionCreated);
            }

            IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(firstIndex)
                .setRoutingNumShards(routingNumShards);

            // Set up everything, now locally create the index to see that things are ok, and apply
            final IndexMetadata tmpImd = tmpImdBuilder.settings(Settings.builder()
                    .put(commonIndexSettings)
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())).build();
            ActiveShardCount waitForActiveShards = tmpImd.getWaitForActiveShards();
            if (!waitForActiveShards.validate(tmpImd.getNumberOfReplicas())) {
                throw new IllegalArgumentException("invalid wait_for_active_shards[" + waitForActiveShards +
                    "]: cannot be greater than number of shard copies [" +
                    (tmpImd.getNumberOfReplicas() + 1) + "]");
            }
            // create the index here (on the master) to validate it can be created, as well as adding the mapping
            IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList(), false);
            testIndex = indexService.index();

            // "Probe" creation of the first index passed validation. Now add all indices to the cluster state metadata and update routing.

            final MappingMetadata mapping = new MappingMetadata(Map.of("default", MappingUtil.createMapping(
                MappingUtil.AllocPosition.forTable(docTableInfo),
                table.pkConstraintName(),
                table.columns(),
                table.primaryKeys(),
                table.checkConstraints(),
                table.partitionedBy(),
                table.columnPolicy(),
                table.routingColumn()
            )));

            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
            ArrayList<String> indexUUIDs = new ArrayList<>(partitions.size());
            for (PartitionName partition : partitions) {
                String indexName = partition.asIndexName();
                final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
                    .setRoutingNumShards(routingNumShards)
                    .partitionValues(partition.values())
                    .settings(Settings.builder()
                        .put(commonIndexSettings)
                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    );

                indexMetadataBuilder.putMapping(mapping);
                indexMetadataBuilder.state(IndexMetadata.State.OPEN);

                final IndexMetadata indexMetadata;
                try {
                    indexMetadata = indexMetadataBuilder.build();
                } catch (Exception e) {
                    removalReason = "failed to build index metadata";
                    throw e;
                }

                logger.info("[{}] creating partition, cause [bulk], shards [{}]/[{}]",
                    partition, indexMetadata.getNumberOfShards(), indexMetadata.getNumberOfReplicas());

                indexService.getIndexEventListener().beforeIndexAddedToCluster(
                    indexMetadata.getIndex(), indexMetadata.getSettings());
                newMetadataBuilder.put(indexMetadata, false);
                indexUUIDs.add(indexMetadata.getIndexUUID());
                routingTableBuilder.addAsNew(indexMetadata);
            }

            newMetadataBuilder.addIndexUUIDs(table, indexUUIDs);
            Metadata newMetadata = newMetadataBuilder.build();

            ClusterState updatedState = ClusterState.builder(currentState)
                .metadata(newMetadata)
                .routingTable(routingTableBuilder.build())
                .build();

            return allocationService.reroute(updatedState, "bulk-index-creation");
        } finally {
            if (testIndex != null) {
                // "probe" index used for validation was partially created - need to clean up
                indicesService.removeIndex(
                    testIndex,
                    IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED,
                    removalReason != null ? removalReason : "failed to create index"
                );
            }
        }
    }

    public void upgradeTemplates(Metadata metadata, Metadata.Builder newMetadataBuilder) {
        applyUpgrader(
            metadata.templates(),
            metadataUpgradeService::upgradeTemplates,
            newMetadataBuilder::removeTemplate,
            (_, indexTemplateMetadata) -> newMetadataBuilder.put(indexTemplateMetadata)
        );
    }

    @VisibleForTesting
    void createIndices(final CreatePartitionsRequest request,
                       final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
            "bulk-create-indices",
            request,
            ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
            (currentState, tasks) -> {
                ClusterStateTaskExecutor.ClusterTasksResult.Builder<CreatePartitionsRequest> builder = ClusterStateTaskExecutor.ClusterTasksResult.builder();
                for (CreatePartitionsRequest request1 : tasks) {
                    try {
                        RelationMetadata.Table table = currentState.metadata().getRelation(request1.relationName());
                        if (table == null) {
                            throw new RelationUnknown(request1.relationName());
                        }
                        DocTableInfo docTableInfo = docTableInfoFactory.create(table.name(), currentState.metadata());
                        currentState = executeCreateIndices(currentState, table, docTableInfo, request1);
                        builder.success(request);
                    } catch (Exception e) {
                        builder.failure(request, e);
                    }
                }
                return builder.build(currentState);
            },
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    RelationMetadata.Table table = currentState.metadata().getRelation(request.relationName());
                    if (table == null) {
                        throw new RelationUnknown(request.relationName());
                    }
                    DocTableInfo docTableInfo = docTableInfoFactory.create(table.name(), currentState.metadata());
                    return executeCreateIndices(currentState, table, docTableInfo, request);
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }
            }
        );
    }

    private static void validatePartitions(ClusterState state,
                                           RelationMetadata.Table table,
                                           List<PartitionName> partitionNames) {
        List<PartitionName> existingPartitions = state.metadata().getIndices(
            table.name(),
            List.of(),
            false,
            im -> new PartitionName(table.name(), im.partitionValues())
        );

        for (PartitionName partition : partitionNames) {
            if (existingPartitions.contains(partition)) {
                continue;
            }
            String indexName = partition.asIndexName();
            if (state.routingTable().hasIndex(indexName)) {
                continue;
            }
            IndexName.validate(indexName);
        }
    }

    private Settings createCommonIndexSettings(ClusterState currentState, Settings tableSettings) {
        Settings.Builder indexSettingsBuilder = Settings.builder();

        indexSettingsBuilder.put(tableSettings);

        Version minVersion = currentState.nodes().getSmallestNonClientNodeVersion();
        indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), minVersion);

        if (indexSettingsBuilder.get(IndexMetadata.SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
        }
        return indexSettingsBuilder.build();
    }

    @Override
    protected ClusterBlockException checkBlock(CreatePartitionsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            indexNames(request.partitionNames(), state).toArray(String[]::new)
        );
    }

    public List<String> indexNames(List<PartitionName> partitionNames, ClusterState state) {
        List<String> indexNames = new ArrayList<>(partitionNames.size());
        for (PartitionName partition : partitionNames) {
            indexNames.addAll(state.metadata().getIndices(
                partition.relationName(),
                partition.values(),
                false,
                im -> im.getIndex().getName()
            ));
        }

        return indexNames;
    }
}
