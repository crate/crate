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
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.crate.metadata.PartitionName;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasValidator;
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
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
public class TransportCreatePartitionsAction
    extends TransportMasterNodeAction<CreatePartitionsRequest, CreatePartitionsResponse> {

    public static final String NAME = "indices:admin/bulk_create";

    private final AliasValidator aliasValidator;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final NamedXContentRegistry xContentRegistry;
    private final Environment environment;
    private final BulkActiveShardsObserver activeShardsObserver;
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
    public TransportCreatePartitionsAction(Settings settings,
                                           TransportService transportService,
                                           Environment environment,
                                           ClusterService clusterService,
                                           ThreadPool threadPool,
                                           AliasValidator aliasValidator,
                                           IndicesService indicesService,
                                           AllocationService allocationService,
                                           NamedXContentRegistry xContentRegistry,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, CreatePartitionsRequest::new);
        this.environment = environment;
        this.aliasValidator = aliasValidator;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.xContentRegistry = xContentRegistry;
        this.activeShardsObserver = new BulkActiveShardsObserver(settings, clusterService, threadPool);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected CreatePartitionsResponse newResponse() {
        return new CreatePartitionsResponse();
    }

    @Override
    protected void masterOperation(final CreatePartitionsRequest request,
                                   final ClusterState state,
                                   final ActionListener<CreatePartitionsResponse> listener) throws ElasticsearchException {

        if (request.indices().isEmpty()) {
            listener.onResponse(new CreatePartitionsResponse(true));
            return;
        }

        createIndices(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                activeShardsObserver.waitForActiveShards(request.indices(), ActiveShardCount.DEFAULT, request.ackTimeout(),
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
                        listener.onResponse(new CreatePartitionsResponse(response.isAcknowledged()));
                    }, listener::onFailure);
            } else {
                logger.warn("[{}] Table partitions created, but publishing new cluster state timed out. Timeout={}",
                    request.indices(), request.timeout());
                listener.onResponse(new CreatePartitionsResponse(false));
            }
        }, listener::onFailure));
    }

    /**
     * This code is more or less the same as the stuff in {@link MetaDataCreateIndexService}
     * but optimized for bulk operation without separate mapping/alias/index settings.
     */
    ClusterState executeCreateIndices(ClusterState currentState, CreatePartitionsRequest request) throws Exception {
        List<String> indicesToCreate = new ArrayList<>(request.indices().size());
        List<String> removalReasons = new ArrayList<>(request.indices().size());
        List<Index> createdIndices = new ArrayList<>(request.indices().size());
        try {
            validateAndFilterExistingIndices(currentState, indicesToCreate, request);
            if (indicesToCreate.isEmpty()) {
                return currentState;
            }

            Map<String, IndexMetaData.Custom> customs = new HashMap<>();
            Map<String, Map<String, Object>> mappings = new HashMap<>();
            Map<String, AliasMetaData> templatesAliases = new HashMap<>();
            List<String> templateNames = new ArrayList<>();

            List<IndexTemplateMetaData> templates = findTemplates(request, currentState);
            applyTemplates(customs, mappings, templatesAliases, templateNames, templates);
            File mappingsDir = new File(environment.configFile().toFile(), "mappings");
            if (mappingsDir.isDirectory()) {
                addMappingFromMappingsFile(mappings, mappingsDir, request);
            }

            MetaData.Builder newMetaDataBuilder = MetaData.builder(currentState.metaData());
            for (String index : indicesToCreate) {
                Settings indexSettings = createIndexSettings(currentState, templates);
                int routingNumShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);

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

                // the context is only used for validation so it's fine to pass fake values for the shard id and the current
                // timestamp
                QueryShardContext queryShardContext = indexService.newQueryShardContext(0, null, () -> 0L);
                for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                    if (aliasMetaData.filter() != null) {
                        aliasValidator.validateAliasFilter(
                            aliasMetaData.alias(), aliasMetaData.filter().uncompressed(), queryShardContext, xContentRegistry);
                    }
                }

                // now, update the mappings with the actual source
                Map<String, MappingMetaData> mappingsMetaData = Maps.newHashMap();
                for (DocumentMapper mapper : mapperService.docMappers(true)) {
                    MappingMetaData mappingMd = new MappingMetaData(mapper);
                    mappingsMetaData.put(mapper.type(), mappingMd);
                }

                final IndexMetaData.Builder indexMetaDataBuilder =
                    IndexMetaData.builder(index).settings(indexSettings);

                for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                    indexMetaDataBuilder.putMapping(mappingMd);
                }
                for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                    indexMetaDataBuilder.putAlias(aliasMetaData);
                }
                for (Map.Entry<String, IndexMetaData.Custom> customEntry : customs.entrySet()) {
                    indexMetaDataBuilder.putCustom(customEntry.getKey(), customEntry.getValue());
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
            updatedState = allocationService.reroute(
                ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(), "bulk-index-creation");
            return updatedState;
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

    private void createIndices(final CreatePartitionsRequest request,
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

    private void addMappingFromMappingsFile(Map<String, Map<String, Object>> mappings, File mappingsDir, CreatePartitionsRequest request) {
        for (String index : request.indices()) {
            // first index level
            File indexMappingsDir = new File(mappingsDir, index);
            if (indexMappingsDir.isDirectory()) {
                addMappings(mappings, indexMappingsDir);
            }

            // second is the _default mapping
            File defaultMappingsDir = new File(mappingsDir, "_default");
            if (defaultMappingsDir.isDirectory()) {
                addMappings(mappings, defaultMappingsDir);
            }
        }
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
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS,
                settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5));
        }
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_NUMBER_OF_REPLICAS) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS,
                settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1));
        }
        if (settings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS) != null
            && indexSettingsBuilder.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS,
                settings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS));
        }

        if (indexSettingsBuilder.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
            DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.smallest(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetaData.SETTING_VERSION_CREATED, createdVersion);
        }

        if (indexSettingsBuilder.get(IndexMetaData.SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
        }

        indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        return indexSettingsBuilder.build();
    }

    private void addMappings(Map<String, Map<String, Object>> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        assert mappingsFiles != null : "file list of the mapping directory should not be null";
        for (File mappingFile : mappingsFiles) {
            if (mappingFile.isHidden()) {
                continue;
            }
            int lastDotIndex = mappingFile.getName().lastIndexOf('.');
            String mappingType =
                lastDotIndex != -1 ? mappingFile.getName().substring(0, lastDotIndex) : mappingFile.getName();
            try {
                String mappingSource = Streams.copyToString(new InputStreamReader(new FileInputStream(mappingFile), Charsets.UTF_8));
                if (mappings.containsKey(mappingType)) {
                    XContentHelper.mergeDefaults(mappings.get(mappingType), parseMapping(mappingSource));
                } else {
                    mappings.put(mappingType, parseMapping(mappingSource));
                }
            } catch (Exception e) {
                logger.warn("failed to read / parse mapping [" + mappingType + "] from location [" + mappingFile +
                            "], ignoring...", e);
            }
        }
    }

    private void applyTemplates(Map<String, IndexMetaData.Custom> customs,
                                Map<String, Map<String, Object>> mappings,
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
            // handle custom
            for (ObjectObjectCursor<String, IndexMetaData.Custom> cursor : template.customs()) {
                String type = cursor.key;
                IndexMetaData.Custom custom = cursor.value;
                IndexMetaData.Custom existing = customs.get(type);
                if (existing == null) {
                    customs.put(type, custom);
                } else {
                    IndexMetaData.Custom merged = existing.mergeWith(custom);
                    customs.put(type, merged);
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
            if (Regex.simpleMatch(template.template(), firstIndex)) {
                templates.add(template);
            }
        }
        CollectionUtil.timSort(templates, (o1, o2) -> o2.order() - o1.order());
        return templates;
    }

    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        try (XContentParser parser = XContentFactory.xContent(mappingSource).createParser(xContentRegistry, mappingSource)) {
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
