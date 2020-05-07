/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.crate.metadata.PartitionName;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
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
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;


/**
 * Creates one or more partitions within one cluster-state-update-task
 * <p>
 * This is more or less a more optimized version of {@link MetaDataCreateIndexService}
 * <p>
 * It also has some limitations:
 * <p>
 * - all indices must actually have the same name pattern (only the first index is used to figure out which templates to use),
 *   which must be the case for partitions anyway.
 * - and alias / mappings / etc. are not taken from the request
 */
@Singleton
public class TransportCreatePartitionsAction extends TransportMasterNodeAction<CreatePartitionsRequest, AcknowledgedResponse> {

    public static final String NAME = "indices:admin/bulk_create";

    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final NamedXContentRegistry xContentRegistry;
    private final ActiveShardsObserver activeShardsObserver;
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
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(NAME, transportService, clusterService, threadPool, CreatePartitionsRequest::new, indexNameExpressionResolver);
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.xContentRegistry = xContentRegistry;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
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
                            IndexTemplateMetaData templateMetaData = state.metaData().getTemplates().get(partitionTemplateName);

                            logger.info("[{}] Table partitions created, but the operation timed out while waiting for " +
                                         "enough shards to be started. Timeout={}, wait_for_active_shards={}. " +
                                         "Consider decreasing the 'number_of_shards' table setting (currently: {}) or adding nodes to the cluster.",
                                request.indices(), request.timeout(),
                                SETTING_WAIT_FOR_ACTIVE_SHARDS.get(templateMetaData.getSettings()),
                                INDEX_NUMBER_OF_SHARDS_SETTING.get(templateMetaData.getSettings()));
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
     * This code is more or less the same as the stuff in {@link MetaDataCreateIndexService}
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

            Map<String, Map<String, Object>> mappings = new HashMap<>();
            Map<String, AliasMetaData> templatesAliases = new HashMap<>();
            List<String> templateNames = new ArrayList<>();

            List<IndexTemplateMetaData> templates = findTemplates(request, currentState);
            applyTemplates(mappings, templatesAliases, templateNames, templates);

            MetaData.Builder newMetaDataBuilder = MetaData.builder(currentState.metaData());
            for (String index : indicesToCreate) {
                Settings indexSettings = createIndexSettings(currentState, templates);
                int routingNumShards = IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings);

                String testIndex = indicesToCreate.get(0);
                IndexMetaData.Builder tmpImdBuilder = IndexMetaData.builder(testIndex)
                    .setRoutingNumShards(routingNumShards);

                // Set up everything, now locally create the index to see that things are ok, and apply
                final IndexMetaData tmpImd = tmpImdBuilder.settings(indexSettings).build();
                ActiveShardCount waitForActiveShards = tmpImd.getWaitForActiveShards();
                if (!waitForActiveShards.validate(tmpImd.getNumberOfReplicas())) {
                    throw new IllegalArgumentException("invalid wait_for_active_shards[" + waitForActiveShards +
                                                       "]: cannot be greater than number of shard copies [" +
                                                       (tmpImd.getNumberOfReplicas() + 1) + "]");
                }
                // create the index here (on the master) to validate it can be created, as well as adding the mapping
                IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList());
                createdIndices.add(indexService.index());

                // now add the mappings
                MapperService mapperService = indexService.mapperService();
                try {
                    mapperService.merge(mappings, MapperService.MergeReason.MAPPING_UPDATE, true);
                } catch (MapperParsingException mpe) {
                    removalReasons.add("failed on parsing mappings on index creation");
                    throw mpe;
                }

                // now, update the mappings with the actual source
                Map<String, MappingMetaData> mappingsMetaData = Maps.newHashMap();
                for (DocumentMapper mapper : mapperService.docMappers(true)) {
                    MappingMetaData mappingMd = new MappingMetaData(mapper);
                    mappingsMetaData.put(mapper.type(), mappingMd);
                }

                final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(index)
                    .setRoutingNumShards(routingNumShards)
                    .settings(indexSettings);

                for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                    indexMetaDataBuilder.putMapping(mappingMd);
                }
                for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                    indexMetaDataBuilder.putAlias(aliasMetaData);
                }
                indexMetaDataBuilder.state(IndexMetaData.State.OPEN);

                final IndexMetaData indexMetaData;
                try {
                    indexMetaData = indexMetaDataBuilder.build();
                } catch (Exception e) {
                    removalReasons.add("failed to build index metadata");
                    throw e;
                }
                logger.info("[{}] creating index, cause [bulk], templates {}, shards [{}]/[{}], mappings {}",
                    index, templateNames, indexMetaData.getNumberOfShards(), indexMetaData.getNumberOfReplicas(), mappings.keySet());

                indexService.getIndexEventListener().beforeIndexAddedToCluster(
                    indexMetaData.getIndex(), indexMetaData.getSettings());
                newMetaDataBuilder.put(indexMetaData, false);
                removalReasons.add("cleaning up after validating index on master");
            }

            MetaData newMetaData = newMetaDataBuilder.build();

            ClusterState updatedState = ClusterState.builder(currentState).metaData(newMetaData).build();
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
            for (String index : indicesToCreate) {
                routingTableBuilder.addAsNew(updatedState.metaData().index(index));
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
                MetaDataCreateIndexService.validateIndexName(index, currentState);
                indicesToCreate.add(index);
            } catch (ResourceAlreadyExistsException e) {
                // ignore
            }
        }
    }

    private Settings createIndexSettings(ClusterState currentState, List<IndexTemplateMetaData> templates) {
        Settings.Builder indexSettingsBuilder = Settings.builder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            indexSettingsBuilder.put(templates.get(i).settings());
        }
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
            DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetaData.SETTING_VERSION_CREATED, createdVersion);
        }
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
        }
        indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        return indexSettingsBuilder.build();
    }

    private void applyTemplates(Map<String, Map<String, Object>> mappings,
                                Map<String, AliasMetaData> templatesAliases,
                                List<String> templateNames,
                                List<IndexTemplateMetaData> templates) throws Exception {

        for (IndexTemplateMetaData template : templates) {
            templateNames.add(template.getName());
            for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                if (mappings.containsKey(cursor.key)) {
                    XContentHelper.mergeDefaults(mappings.get(cursor.key), parseMapping(cursor.value.string()));
                } else {
                    mappings.put(cursor.key, parseMapping(cursor.value.string()));
                }
            }
            //handle aliases
            for (ObjectObjectCursor<String, AliasMetaData> cursor : template.aliases()) {
                AliasMetaData aliasMetaData = cursor.value;
                templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
            }
        }
    }

    private List<IndexTemplateMetaData> findTemplates(CreatePartitionsRequest request, ClusterState state) {
        List<IndexTemplateMetaData> templates = new ArrayList<>();
        String firstIndex = request.indices().iterator().next();


        // note: only use the first index name to see if template matches.
        // this means
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData template = cursor.value;
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
