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
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.validateSoftDeletesSetting;
import static org.elasticsearch.gateway.GatewayMetaState.applyPluginUpgraders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.crate.metadata.IndexName;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;


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
@Singleton
public class TransportCreatePartitionsAction extends TransportMasterNodeAction<CreatePartitionsRequest, AcknowledgedResponse> {

    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final ActiveShardsObserver activeShardsObserver;
    private final ShardLimitValidator shardLimitValidator;
    private final MetadataUpgrader metadataUpgrader;
    private final ClusterStateTaskExecutor<CreatePartitionsRequest> executor = (currentState, tasks) -> {
        ClusterStateTaskExecutor.ClusterTasksResult.Builder<CreatePartitionsRequest> builder = ClusterStateTaskExecutor.ClusterTasksResult.builder();
        for (CreatePartitionsRequest request : tasks) {
            try {
                currentState = executeCreateIndices(currentState, request);
                builder.success(request);
            } catch (Exception e) {
                builder.failure(request, e);
            }
        }
        return builder.build(currentState);
    };

    private final AtomicBoolean upgraded;

    @Inject
    public TransportCreatePartitionsAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           IndicesService indicesService,
                                           AllocationService allocationService,
                                           ShardLimitValidator shardLimitValidator,
                                           MetadataUpgrader metadataUpgrader) {
        super(CreatePartitionsAction.NAME, transportService, clusterService, threadPool, CreatePartitionsRequest::new);
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
        this.shardLimitValidator = shardLimitValidator;
        this.metadataUpgrader = metadataUpgrader;
        this.upgraded = new AtomicBoolean(false);
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
                List<String> indexNames = request.indexNames();
                activeShardsObserver.waitForActiveShards(indexNames.toArray(String[]::new), ActiveShardCount.DEFAULT, request.ackTimeout(),
                    shardsAcked -> {
                        if (!shardsAcked && logger.isInfoEnabled()) {
                            RelationName relationName = request.relationName();
                            String partitionTemplateName = PartitionName.templateName(relationName.schema(), relationName.name());
                            IndexTemplateMetadata templateMetadata = state.metadata().templates().get(partitionTemplateName);

                            logger.info("[{}] Table partitions created, but the operation timed out while waiting for " +
                                         "enough shards to be started. Timeout={}, wait_for_active_shards={}. " +
                                         "Consider decreasing the 'number_of_shards' table setting (currently: {}) or adding nodes to the cluster.",
                                relationName, request.timeout(),
                                SETTING_WAIT_FOR_ACTIVE_SHARDS.get(templateMetadata.settings()),
                                INDEX_NUMBER_OF_SHARDS_SETTING.get(templateMetadata.settings()));
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
    public ClusterState executeCreateIndices(ClusterState currentState, CreatePartitionsRequest request) throws Exception {
        String removalReason = null;
        Index testIndex = null;
        try {
            List<String> indicesToCreate = getValidatedIndicesToCreate(currentState, request);
            if (indicesToCreate.isEmpty()) {
                return currentState;
            }

            // We always have only 1 matching template per pattern/table.
            // All indices in the request are related to a concrete partitioned table and
            // they all match the same template. Thus, we can use any of them to find matching template.
            String firstIndex = indicesToCreate.get(0);
            String templateName = PartitionName.templateName(firstIndex);


            final Metadata metadata = currentState.metadata();
            Metadata.Builder newMetadataBuilder = Metadata.builder(metadata);

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

            IndexTemplateMetadata template = newMetadataBuilder.getTemplate(templateName);
            if (template == null) {
                // Normally should be impossible, as it would mean that we are inserting into a partitioned table without template,
                // i.e inserting after CREATE TABLE failed
                throw new IllegalStateException(String.format(Locale.ENGLISH, "Cannot find a template for partitioned table's index %s", firstIndex));
            }

            // Use only first index to validate that index can be created.
            // All indices share same template/settings so no need to repeat validation for each index.
            Settings commonIndexSettings = createCommonIndexSettings(currentState, template);

            final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(commonIndexSettings);
            final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(commonIndexSettings);
            // Shard limit check has to take to account all new partitions.
            final int numShardsToCreate = numberOfShards * (1 + numberOfReplicas) * indicesToCreate.size();
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

            MappingMetadata mappingMetadata = new MappingMetadata(template.mapping());
            for (String index : indicesToCreate) {
                final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index)
                    .setRoutingNumShards(routingNumShards)
                    .settings(Settings.builder()
                        .put(commonIndexSettings)
                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    );

                indexMetadataBuilder.putMapping(mappingMetadata);
                for (var aliasCursor : template.aliases().values()) {
                    indexMetadataBuilder.putAlias(aliasCursor.value);
                }
                indexMetadataBuilder.state(IndexMetadata.State.OPEN);

                final IndexMetadata indexMetadata;
                try {
                    indexMetadata = indexMetadataBuilder.build();
                } catch (Exception e) {
                    removalReason = "failed to build index metadata";
                    throw e;
                }

                logger.info("[{}] creating index, cause [bulk], template {}, shards [{}]/[{}]",
                    index, templateName, indexMetadata.getNumberOfShards(), indexMetadata.getNumberOfReplicas());

                indexService.getIndexEventListener().beforeIndexAddedToCluster(
                    indexMetadata.getIndex(), indexMetadata.getSettings());
                newMetadataBuilder.put(indexMetadata, false);
            }

            Metadata newMetadata = newMetadataBuilder.build();

            ClusterState updatedState = ClusterState.builder(currentState).metadata(newMetadata).build();
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
            for (String index : indicesToCreate) {
                routingTableBuilder.addAsNew(updatedState.metadata().index(index));
            }
            return allocationService.reroute(
                ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(), "bulk-index-creation");
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
        applyPluginUpgraders(
            metadata.templates(),
            metadataUpgrader.indexTemplateMetadataUpgraders,
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
            executor,
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return executeCreateIndices(currentState, request);
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }
            }
        );
    }

    private List<String> getValidatedIndicesToCreate(ClusterState state, CreatePartitionsRequest request) {
        ArrayList<String> indicesToCreate = new ArrayList<>(request.partitionValuesList().size());
        for (List<String> partitionValues : request.partitionValuesList()) {
            String index = new PartitionName(request.relationName(), partitionValues).asIndexName();
            if (state.metadata().hasIndex(index)) {
                continue;
            }
            if (state.routingTable().hasIndex(index)) {
                continue;
            }
            IndexName.validate(index);
            indicesToCreate.add(index);
        }
        return indicesToCreate;
    }

    private Settings createCommonIndexSettings(ClusterState currentState, @Nullable IndexTemplateMetadata template) {
        Version minVersion = currentState.nodes().getSmallestNonClientNodeVersion();
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), minVersion);

        // apply template
        if (template != null) {
            indexSettingsBuilder.put(template.settings());
        }

        validateSoftDeletesSetting(indexSettingsBuilder.build());

        if (indexSettingsBuilder.get(IndexMetadata.SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
        }
        return indexSettingsBuilder.build();
    }

    @Override
    protected ClusterBlockException checkBlock(CreatePartitionsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            request.indexNames().toArray(String[]::new)
        );
    }
}
