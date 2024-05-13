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

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.collections.Maps;
import io.crate.server.xcontent.XContentHelper;

/**
 * Service responsible for submitting mapping changes
 */
public class MetadataMappingService {

    private static final Logger LOGGER = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    final RefreshTaskExecutor refreshExecutor = new RefreshTaskExecutor();


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
            DocumentMapper mapper = indexService.mapperService().documentMapper();
            if (mapper != null && !mapper.mappingSource().equals(builder.mapping().source())) {
                dirty = true;
                LOGGER.warn("[{}] re-syncing mappings with cluster state", index);
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

    public static void populateColumnPositions(Map<String, Object> mapping, CompressedXContent mappingToReference) {
        Map<String, Object> parsedTemplateMapping = XContentHelper.convertToMap(mappingToReference.compressedReference(), true, XContentType.JSON).map();
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
            templateColumnProperties = Maps.getOrDefault(templateColumnProperties, "inner", templateColumnProperties);
            indexColumnProperties = Maps.getOrDefault(indexColumnProperties, "inner", indexColumnProperties);

            Integer templateChildPosition = (Integer) templateColumnProperties.get("position");
            assert templateColumnProperties.containsKey("position") && templateChildPosition != null : "the template mapping is missing column positions";

            // BWC compatibility with nodes < 5.1, position could be NULL if column is created on that nodes
            if (templateChildPosition != null) {
                // since template mapping and index mapping should be consistent, simply override (this will resolve any duplicates in index mappings)
                indexColumnProperties.put("position", templateChildPosition);
            }

            populateColumnPositionsImpl(indexColumnProperties, templateColumnProperties);
        }
    }
}
