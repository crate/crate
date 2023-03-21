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
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.setIndexVersionCreatedSetting;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.validateSoftDeletesSetting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.cluster.metadata.AliasMetadata;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Iterables;
import io.crate.metadata.PartitionName;


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
    private final NamedXContentRegistry xContentRegistry;
    private final ActiveShardsObserver activeShardsObserver;
    private final ShardLimitValidator shardLimitValidator;
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

    @Inject
    public TransportCreatePartitionsAction(TransportService transportService,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           IndicesService indicesService,
                                           AllocationService allocationService,
                                           NamedXContentRegistry xContentRegistry,
                                           ShardLimitValidator shardLimitValidator) {
        super(CreatePartitionsAction.NAME, transportService, clusterService, threadPool, CreatePartitionsRequest::new);
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.xContentRegistry = xContentRegistry;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
        this.shardLimitValidator = shardLimitValidator;
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

        if (request.indices().isEmpty()) {
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }

        createIndices(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                activeShardsObserver.waitForActiveShards(request.indices().toArray(new String[0]), ActiveShardCount.DEFAULT, request.ackTimeout(),
                    shardsAcked -> {
                        if (!shardsAcked && logger.isInfoEnabled()) {
                            String partitionTemplateName = PartitionName.templateName(request.indices().iterator().next());
                            IndexTemplateMetadata templateMetadata = state.metadata().templates().get(partitionTemplateName);

                            logger.info("[{}] Table partitions created, but the operation timed out while waiting for " +
                                         "enough shards to be started. Timeout={}, wait_for_active_shards={}. " +
                                         "Consider decreasing the 'number_of_shards' table setting (currently: {}) or adding nodes to the cluster.",
                                request.indices(), request.timeout(),
                                SETTING_WAIT_FOR_ACTIVE_SHARDS.get(templateMetadata.getSettings()),
                                INDEX_NUMBER_OF_SHARDS_SETTING.get(templateMetadata.getSettings()));
                        }
                        listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
                    }, listener::onFailure);
            } else {
                logger.warn("[{}] Table partitions created, but publishing new cluster state timed out. Timeout={}",
                    request.indices(), request.timeout());
                listener.onResponse(new AcknowledgedResponse(false));
            }
        }, listener::onFailure));
    }

    /**
     * This code is more or less the same as the stuff in {@link MetadataCreateIndexService}
     * but optimized for bulk operation without separate mapping/alias/index settings.
     */
    private ClusterState executeCreateIndices(ClusterState currentState, CreatePartitionsRequest request) throws Exception {
        List<String> indicesToCreate = new ArrayList<>(request.indices().size());
        List<String> removalReasons = new ArrayList<>(request.indices().size());
        List<Index> createdIndices = new ArrayList<>(request.indices().size());
        try {
            validateAndFilterExistingIndices(currentState, indicesToCreate, request);
            if (indicesToCreate.isEmpty()) {
                return currentState;
            }

            Map<String, Object> mapping = new HashMap<>();
            Map<String, AliasMetadata> templatesAliases = new HashMap<>();
            List<String> templateNames = new ArrayList<>();

            List<IndexTemplateMetadata> templates = findTemplates(request, currentState);
            applyTemplates(mapping, templatesAliases, templateNames, templates);

            Metadata.Builder newMetadataBuilder = Metadata.builder(currentState.metadata());
            for (String index : indicesToCreate) {
                Settings indexSettings = createIndexSettings(currentState, templates);
                shardLimitValidator.validateShardLimit(indexSettings, currentState);
                int routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings);

                String testIndex = indicesToCreate.get(0);
                IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(testIndex)
                    .setRoutingNumShards(routingNumShards);

                // Set up everything, now locally create the index to see that things are ok, and apply
                final IndexMetadata tmpImd = tmpImdBuilder.settings(indexSettings).build();
                ActiveShardCount waitForActiveShards = tmpImd.getWaitForActiveShards();
                if (!waitForActiveShards.validate(tmpImd.getNumberOfReplicas())) {
                    throw new IllegalArgumentException("invalid wait_for_active_shards[" + waitForActiveShards +
                                                       "]: cannot be greater than number of shard copies [" +
                                                       (tmpImd.getNumberOfReplicas() + 1) + "]");
                }
                // create the index here (on the master) to validate it can be created, as well as adding the mapping
                IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList(), false);
                createdIndices.add(indexService.index());

                // now add the mappings
                MapperService mapperService = indexService.mapperService();
                if (!mapping.isEmpty()) {
                    try {
                        mapperService.merge(mapping, MapperService.MergeReason.MAPPING_UPDATE);
                    } catch (MapperParsingException mpe) {
                        removalReasons.add("failed on parsing mappings on index creation");
                        throw mpe;
                    }
                }

                // now, update the mappings with the actual source
                final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index)
                    .setRoutingNumShards(routingNumShards)
                    .settings(indexSettings);

                DocumentMapper mapper = mapperService.documentMapper();
                if (mapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(mapper));
                }

                for (AliasMetadata aliasMetadata : templatesAliases.values()) {
                    indexMetadataBuilder.putAlias(aliasMetadata);
                }
                indexMetadataBuilder.state(IndexMetadata.State.OPEN);

                final IndexMetadata indexMetadata;
                try {
                    indexMetadata = indexMetadataBuilder.build();
                } catch (Exception e) {
                    removalReasons.add("failed to build index metadata");
                    throw e;
                }


                logger.info("[{}] creating index, cause [bulk], templates {}, shards [{}]/[{}]",
                    index, templateNames, indexMetadata.getNumberOfShards(), indexMetadata.getNumberOfReplicas());

                indexService.getIndexEventListener().beforeIndexAddedToCluster(
                    indexMetadata.getIndex(), indexMetadata.getSettings());
                newMetadataBuilder.put(indexMetadata, false);
                removalReasons.add("cleaning up after validating index on master");
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
            for (int i = 0; i < createdIndices.size(); i++) {
                // Index was already partially created - need to clean up
                String removalReason = removalReasons.size() > i ? removalReasons.get(i) : "failed to create index";
                indicesService.removeIndex(
                    createdIndices.get(i),
                    IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED,
                    removalReason);
            }
        }
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

    private void validateAndFilterExistingIndices(ClusterState currentState,
                                                  List<String> indicesToCreate,
                                                  CreatePartitionsRequest request) {
        for (String index : request.indices()) {
            try {
                MetadataCreateIndexService.validateIndexName(index, currentState);
                indicesToCreate.add(index);
            } catch (ResourceAlreadyExistsException e) {
                // ignore
            }
        }
    }

    private Settings createIndexSettings(ClusterState currentState, List<IndexTemplateMetadata> templates) {
        Settings.Builder indexSettingsBuilder = Settings.builder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            indexSettingsBuilder.put(templates.get(i).settings());
        }

        setIndexVersionCreatedSetting(indexSettingsBuilder, currentState);
        validateSoftDeletesSetting(indexSettingsBuilder.build());

        if (indexSettingsBuilder.get(IndexMetadata.SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
        }
        indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        return indexSettingsBuilder.build();
    }

    private void applyTemplates(Map<String, Object> mapping,
                                Map<String, AliasMetadata> templatesAliases,
                                List<String> templateNames,
                                List<IndexTemplateMetadata> templates) throws Exception {

        for (IndexTemplateMetadata template : templates) {
            templateNames.add(template.getName());
            XContentHelper.mergeDefaults(mapping, parseMapping(template.mapping().string()));
            //handle aliases
            for (ObjectObjectCursor<String, AliasMetadata> cursor : template.aliases()) {
                AliasMetadata aliasMetadata = cursor.value;
                templatesAliases.put(aliasMetadata.alias(), aliasMetadata);
            }
        }
    }

    private List<IndexTemplateMetadata> findTemplates(CreatePartitionsRequest request, ClusterState state) {
        List<IndexTemplateMetadata> templates = new ArrayList<>();
        String firstIndex = request.indices().iterator().next();


        // note: only use the first index name to see if template matches.
        // this means
        for (ObjectCursor<IndexTemplateMetadata> cursor : state.metadata().templates().values()) {
            IndexTemplateMetadata template = cursor.value;
            for (String pattern : template.getPatterns()) {
                if (Regex.simpleMatch(pattern, firstIndex)) {
                    templates.add(template);
                    break;
                }
            }
        }
        CollectionUtil.timSort(templates, (o1, o2) -> o2.order() - o1.order());
        return templates;
    }

    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
            .createParser(xContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping", e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(CreatePartitionsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, Iterables.toArray(request.indices(), String.class));
    }
}
