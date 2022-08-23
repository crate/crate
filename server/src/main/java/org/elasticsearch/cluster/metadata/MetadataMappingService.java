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

import io.crate.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import javax.annotation.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;

import io.crate.Constants;
import io.crate.common.collections.Maps;
import io.crate.common.unit.TimeValue;
import io.crate.common.io.IOUtils;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.metadata.doc.DocIndexMetadata.furtherColumnProperties;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting mapping changes
 */
public class MetadataMappingService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    final RefreshTaskExecutor refreshExecutor = new RefreshTaskExecutor();
    public final PutMappingExecutor putMappingExecutor = new PutMappingExecutor();


    @Inject
    public MetadataMappingService(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    static class RefreshTask {
        final String index;
        final String indexUUID;

        RefreshTask(String index, final String indexUUID) {
            this.index = index;
            this.indexUUID = indexUUID;
        }

        @Override
        public String toString() {
            return "[" + index + "][" + indexUUID + "]";
        }
    }

    class RefreshTaskExecutor implements ClusterStateTaskExecutor<RefreshTask> {
        @Override
        public ClusterTasksResult<RefreshTask> execute(ClusterState currentState, List<RefreshTask> tasks) throws Exception {
            ClusterState newClusterState = executeRefresh(currentState, tasks);
            return ClusterTasksResult.<RefreshTask>builder().successes(tasks).build(newClusterState);
        }
    }

    /**
     * Batch method to apply all the queued refresh operations. The idea is to try and batch as much
     * as possible so we won't create the same index all the time for example for the updates on the same mapping
     * and generate a single cluster change event out of all of those.
     */
    ClusterState executeRefresh(final ClusterState currentState, final List<RefreshTask> allTasks) throws Exception {
        // break down to tasks per index, so we can optimize the on demand index service creation
        // to only happen for the duration of a single index processing of its respective events
        Map<String, List<RefreshTask>> tasksPerIndex = new HashMap<>();
        for (RefreshTask task : allTasks) {
            if (task.index == null) {
                LOGGER.debug("ignoring a mapping task of type [{}] with a null index.", task);
            }
            tasksPerIndex.computeIfAbsent(task.index, k -> new ArrayList<>()).add(task);
        }

        boolean dirty = false;
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());

        for (Map.Entry<String, List<RefreshTask>> entry : tasksPerIndex.entrySet()) {
            IndexMetadata indexMetadata = mdBuilder.get(entry.getKey());
            if (indexMetadata == null) {
                // index got deleted on us, ignore...
                LOGGER.debug("[{}] ignoring tasks - index meta data doesn't exist", entry.getKey());
                continue;
            }
            final Index index = indexMetadata.getIndex();
            // the tasks lists to iterate over, filled with the list of mapping tasks, trying to keep
            // the latest (based on order) update mapping one per node
            List<RefreshTask> allIndexTasks = entry.getValue();
            boolean hasTaskWithRightUUID = false;
            for (RefreshTask task : allIndexTasks) {
                if (indexMetadata.isSameUUID(task.indexUUID)) {
                    hasTaskWithRightUUID = true;
                } else {
                    LOGGER.debug("{} ignoring task [{}] - index meta data doesn't match task uuid", index, task);
                }
            }
            if (hasTaskWithRightUUID == false) {
                continue;
            }

            // construct the actual index if needed, and make sure the relevant mappings are there
            boolean removeIndex = false;
            IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
            if (indexService == null) {
                // we need to create the index here, and add the current mapping to it, so we can merge
                indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
                removeIndex = true;
                indexService.mapperService().merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
            }

            IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
            try {
                boolean indexDirty = refreshIndexMapping(indexService, builder);
                if (indexDirty) {
                    mdBuilder.put(builder);
                    dirty = true;
                }
            } finally {
                if (removeIndex) {
                    indicesService.removeIndex(index, NO_LONGER_ASSIGNED, "created for mapping processing");
                }
            }
        }

        if (!dirty) {
            return currentState;
        }
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private boolean refreshIndexMapping(IndexService indexService, IndexMetadata.Builder builder) {
        boolean dirty = false;
        String index = indexService.index().getName();
        try {
            List<String> updatedTypes = new ArrayList<>();
            DocumentMapper mapper = indexService.mapperService().documentMapper();
            if (mapper != null) {
                final String type = mapper.type();
                if (!mapper.mappingSource().equals(builder.mapping(type).source())) {
                    updatedTypes.add(type);
                }
            }

            // if a single type is not up-to-date, re-send everything
            if (updatedTypes.isEmpty() == false) {
                LOGGER.warn("[{}] re-syncing mappings with cluster state because of types [{}]", index, updatedTypes);
                dirty = true;
                if (mapper != null) {
                    builder.putMapping(new MappingMetadata(mapper));
                }
            }
        } catch (Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("[{}] failed to refresh-mapping in cluster state", index), e);
        }
        return dirty;
    }

    /**
     * Refreshes mappings if they are not the same between original and parsed version
     */
    public void refreshMapping(final String index, final String indexUUID) {
        final RefreshTask refreshTask = new RefreshTask(index, indexUUID);
        clusterService.submitStateUpdateTask("refresh-mapping",
            refreshTask,
            ClusterStateTaskConfig.build(Priority.HIGH),
            refreshExecutor,
            (source, e) -> LOGGER.warn(() -> new ParameterizedMessage("failure during [{}]", source), e)
        );
    }

    public class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {
        @Override
        public ClusterTasksResult<PutMappingClusterStateUpdateRequest> execute(ClusterState currentState,
                                                                               List<PutMappingClusterStateUpdateRequest> tasks) throws Exception {
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            ClusterTasksResult.Builder<PutMappingClusterStateUpdateRequest> builder = ClusterTasksResult.builder();
            try {
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    try {
                        for (Index index : request.indices()) {
                            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
                            if (indexMapperServices.containsKey(indexMetadata.getIndex()) == false) {
                                MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
                                indexMapperServices.put(index, mapperService);
                                // add mappings for all types, we need them for cross-type validation
                                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                            }
                        }
                        currentState = applyRequest(currentState, request, indexMapperServices);
                        builder.success(request);
                    } catch (Exception e) {
                        builder.failure(request, e);
                    }
                }
                return builder.build(currentState);
            } finally {
                IOUtils.close(indexMapperServices.values());
            }
        }

        public ClusterState applyRequest(ClusterState currentState, PutMappingClusterStateUpdateRequest request,
                                          Map<Index, MapperService> indexMapperServices) throws Exception {
            CompressedXContent mappingUpdateSource = new CompressedXContent(request.source());
            final Metadata metadata = currentState.metadata();
            final List<IndexMetadata> updateList = new ArrayList<>();
            for (Index index : request.indices()) {
                MapperService mapperService = indexMapperServices.get(index);
                // IMPORTANT: always get the metadata from the state since it get's batched
                // and if we pull it from the indexService we might miss an update etc.
                final IndexMetadata indexMetadata = currentState.getMetadata().getIndexSafe(index);

                // this is paranoia... just to be sure we use the exact same metadata tuple on the update that
                // we used for the validation, it makes this mechanism little less scary (a little)
                updateList.add(indexMetadata);
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                DocumentMapper existingMapper = mapperService.documentMapper();
                DocumentMapper newMapper = mapperService.parse(mappingUpdateSource);
                if (existingMapper != null) {
                    // first, simulate: just call merge and ignore the result
                    existingMapper.merge(newMapper.mapping());
                }
            }
            Metadata.Builder builder = Metadata.builder(metadata);
            boolean updated = false;
            for (IndexMetadata indexMetadata : updateList) {
                boolean updatedMapping = false;
                // do the actual merge here on the master, and update the mapping source
                // we use the exact same indexService and metadata we used to validate above here to actually apply the update
                final Index index = indexMetadata.getIndex();

                Map<String, Object> updatedSourceMap = null;
                if (IndexParts.isPartitioned(index.getName())) {
                    String partitionName = PartitionName.templateName(index.getName());
                    IndexTemplateMetadata indexTemplateMetadata = currentState.metadata().templates().get(partitionName);
                    updatedSourceMap = XContentHelper.convertToMap(mappingUpdateSource.compressedReference(), true).map();
                    populateColumnPositions(updatedSourceMap,
                                            // if partitioned, template-mapping should contain the latest column positions
                                            indexTemplateMetadata.mappings().get(Constants.DEFAULT_MAPPING_TYPE)
                    );
                }


                final MapperService mapperService = indexMapperServices.get(index);

                CompressedXContent existingSource = null;
                DocumentMapper existingMapper = mapperService.documentMapper();
                if (existingMapper != null) {
                    existingSource = existingMapper.mappingSource();
                }
                DocumentMapper mergedMapper =
                    (updatedSourceMap == null /* if partitioned */) ?
                        mapperService.merge(mappingUpdateSource, MergeReason.MAPPING_UPDATE) :
                        mapperService.merge(updatedSourceMap, MergeReason.MAPPING_UPDATE);
                CompressedXContent updatedSource = mergedMapper.mappingSource();

                if (existingSource != null) {
                    if (existingSource.equals(updatedSource)) {
                        // same source, no changes, ignore it
                    } else {
                        updatedMapping = true;
                        // use the merged mapping source
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("{} update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                        } else if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("{} update_mapping [{}]", index, mergedMapper.type());
                        }

                    }
                } else {
                    updatedMapping = true;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("{} create_mapping with source [{}]", index, updatedSource);
                    } else if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("{} create_mapping", index);
                    }
                }

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                // Mapping updates on a single type may have side-effects on other types so we need to
                // update mapping metadata on all types
                DocumentMapper mapper = mapperService.documentMapper();
                if (mapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(mapper.mappingSource()));
                }
                if (updatedMapping) {
                    indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion());
                }
                /*
                 * This implicitly increments the index metadata version and builds the index metadata. This means that we need to have
                 * already incremented the mapping version if necessary. Therefore, the mapping version increment must remain before this
                 * statement.
                 */
                builder.put(indexMetadataBuilder);
                updated |= updatedMapping;
            }
            if (updated) {
                return ClusterState.builder(currentState).metadata(builder).build();
            } else {
                return currentState;
            }
        }

    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("put-mapping",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
                putMappingExecutor,
                new AckedClusterStateTaskListener() {

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        return true;
                    }

                    @Override
                    public void onAllNodesAcked(@Nullable Exception e) {
                        listener.onResponse(new ClusterStateUpdateResponse(e == null));
                    }

                    @Override
                    public void onAckTimeout() {
                        listener.onResponse(new ClusterStateUpdateResponse(false));
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return request.ackTimeout();
                    }
                });
    }

    public static void populateColumnPositions(Map<String, Object> mapping, CompressedXContent mappingToReference) {
        Map<String, Object> parsedTemplateMapping = XContentHelper.convertToMap(mappingToReference.compressedReference(), true).map();
        populateColumnPositionsImpl(
            Maps.getOrDefault(mapping, "default", mapping),
            Maps.getOrDefault(parsedTemplateMapping, "default", parsedTemplateMapping)
        );
    }

    // template mappings must contain up-to-date and correct column positions that all relevant index mappings can reference.
    @VisibleForTesting
    static void populateColumnPositionsImpl(Map<String, Object> indexMapping, Map<String, Object> templateMapping) {
        Map<String, Object> indexProperties = Maps.get(indexMapping, "properties");
        if (indexProperties == null) {
            return;
        }
        Map<String, Object> templateProperties = Maps.get(templateMapping, "properties");
        if (templateProperties == null) {
            templateProperties = Map.of();
        }
        for (var e : indexProperties.entrySet()) {
            String key = e.getKey();
            Map<String, Object> indexColumnProperties = (Map<String, Object>) e.getValue();
            Map<String, Object> templateColumnProperties = (Map<String, Object>) templateProperties.get(key);

            if (templateColumnProperties == null) {
                templateColumnProperties = Map.of();
            }
            templateColumnProperties = furtherColumnProperties(templateColumnProperties);
            indexColumnProperties = furtherColumnProperties(indexColumnProperties);

            Integer templateChildPosition = (Integer) templateColumnProperties.get("position");
            assert templateChildPosition != null : "the template mapping is missing column positions";
            // since template mapping and index mapping should be consistent, simply override (this will resolve any duplicates in index mappings)
            indexColumnProperties.put("position", templateChildPosition);

            populateColumnPositionsImpl(indexColumnProperties, templateColumnProperties);
        }
    }
}
