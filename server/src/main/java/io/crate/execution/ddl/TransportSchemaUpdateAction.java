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

package io.crate.execution.ddl;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.Constants;
import io.crate.action.FutureActionListener;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.index.mapper.MapperService.parseMapping;

@Singleton
public class TransportSchemaUpdateAction extends TransportMasterNodeAction<SchemaUpdateRequest, AcknowledgedResponse> {

    private final NodeClient nodeClient;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportSchemaUpdateAction(TransportService transportService,
                                       ClusterService clusterService,
                                       ThreadPool threadPool,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       NodeClient nodeClient,
                                       NamedXContentRegistry xContentRegistry) {
        super(
            "internal:crate:sql/ddl/schema_update",
            transportService,
            clusterService,
            threadPool,
            SchemaUpdateRequest::new,
            indexNameExpressionResolver
        );
        this.nodeClient = nodeClient;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task,
                                   SchemaUpdateRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        // ideally we'd handle the index mapping update together with the template update in a single clusterStateUpdateTask
        // but the index mapping-update logic is difficult to re-use
        if (IndexParts.isPartitioned(request.index().getName())) {
            updateTemplate(
                state.getMetadata().getTemplates(),
                request.index().getName(),
                request.mappingSource(),
                request.masterNodeTimeout()
            ).thenCompose(r -> updateMapping(request.index(), request.masterNodeTimeout(), request.mappingSource()))
                .thenApply(r -> new AcknowledgedResponse(r.isAcknowledged()))
                .whenComplete(ActionListener.toBiConsumer(listener));
        } else {
            updateMapping(request.index(), request.masterNodeTimeout(), request.mappingSource())
                .thenApply(r -> new AcknowledgedResponse(r.isAcknowledged()))
                .whenComplete(ActionListener.toBiConsumer(listener));
        }
    }

    private CompletableFuture<AcknowledgedResponse> updateMapping(Index index,
                                                                  TimeValue timeout,
                                                                  String mappingSource) {
        FutureActionListener<AcknowledgedResponse, AcknowledgedResponse> putMappingListener = FutureActionListener.newInstance();
        PutMappingRequest putMappingRequest = new PutMappingRequest()
            .indices(new String[0])
            .setConcreteIndex(index)
            .source(mappingSource, XContentType.JSON)
            .timeout(timeout)
            .masterNodeTimeout(timeout);
        nodeClient.execute(PutMappingAction.INSTANCE, putMappingRequest, putMappingListener);
        return putMappingListener;
    }

    private CompletableFuture<AcknowledgedResponse> updateTemplate(ImmutableOpenMap<String, IndexTemplateMetadata> templates,
                                                                   String indexName,
                                                                   String mappingSource,
                                                                   TimeValue timeout) {
        CompletableFuture<AcknowledgedResponse> future = new CompletableFuture<>();
        String templateName = PartitionName.templateName(indexName);
        Map<String, Object> newMapping;
        try {
            XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource);
            newMapping = parser.map();
            if (newMappingAlreadyApplied(templates.get(templateName), newMapping)) {
                return CompletableFuture.completedFuture(new AcknowledgedResponse(true));
            }
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        clusterService.submitStateUpdateTask("update-template-mapping", new ClusterStateUpdateTask(Priority.HIGH) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return updateTemplate(xContentRegistry, currentState, templateName, newMapping);
            }

            @Override
            public TimeValue timeout() {
                return timeout;
            }

            @Override
            public void onFailure(String source, Exception e) {
                future.completeExceptionally(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                future.complete(new AcknowledgedResponse(true));
            }
        });
        return future;
    }

    private boolean newMappingAlreadyApplied(IndexTemplateMetadata template, Map<String, Object> newMapping) throws Exception {
        CompressedXContent defaultMapping = template.getMappings().get(Constants.DEFAULT_MAPPING_TYPE);
        Map<String, Object> currentMapping = parseMapping(xContentRegistry, defaultMapping.toString());
        return !XContentHelper.update(currentMapping, newMapping, true);
    }

    @VisibleForTesting
    static ClusterState updateTemplate(NamedXContentRegistry xContentRegistry,
                                       ClusterState currentState,
                                       String templateName,
                                       Map<String, Object> newMapping) throws Exception {
        IndexTemplateMetadata template = currentState.metadata().templates().get(templateName);
        if (template == null) {
            throw new ResourceNotFoundException("Template \"" + templateName + "\" for partitioned table is missing");
        }

        IndexTemplateMetadata.Builder templateBuilder = new IndexTemplateMetadata.Builder(template);
        for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
            Map<String, Object> source = parseMapping(xContentRegistry, cursor.value.toString());
            mergeIntoSource(source, newMapping);
            try (XContentBuilder xContentBuilder = JsonXContent.contentBuilder()) {
                templateBuilder.putMapping(cursor.key, Strings.toString(xContentBuilder.map(source)));
            }
        }
        Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(templateBuilder);
        return ClusterState.builder(currentState).metadata(builder).build();
    }

    static void mergeIntoSource(Map<String, Object> source, Map<String, Object> mappingUpdate) {
        mergeIntoSource(source, mappingUpdate, Collections.emptyList());
    }

    static void mergeIntoSource(Map<String, Object> source, Map<String, Object> mappingUpdate, List<String> path) {
        for (Map.Entry<String, Object> updateEntry : mappingUpdate.entrySet()) {
            String key = updateEntry.getKey();
            Object updateValue = updateEntry.getValue();
            if (source.containsKey(key)) {
                Object sourceValue = source.get(key);
                if (sourceValue instanceof Map && updateValue instanceof Map) {
                    //noinspection unchecked
                    mergeIntoSource((Map) sourceValue, (Map) updateValue, Lists2.concat(path, key));
                } else {
                    if (updateAllowed(key, sourceValue, updateValue)) {
                        source.put(key, updateValue);
                    } else if (!isUpdateIgnored(path) && !sourceValue.equals(updateValue)) {
                        String fqKey = String.join(".", path) + '.' + key;
                        throw new IllegalArgumentException(
                            "Can't overwrite " + fqKey + "=" + sourceValue + " with " + updateValue);
                    }
                }
            } else {
                source.put(key, updateValue);
            }
        }
    }

    private static boolean isUpdateIgnored(List<String> path) {
        List<String> versionMeta = List.of("default", "_meta", IndexMappings.VERSION_STRING);
        return path.size() > 3 && path.subList(0, 3).equals(versionMeta);
    }

    private static boolean updateAllowed(String key, Object sourceValue, Object updateValue) {
        if (sourceValue instanceof Boolean && updateValue instanceof String && key.equals("dynamic")) {
            return sourceValue.toString().equals(updateValue);
        }
        return false;
    }

    @Override
    protected ClusterBlockException checkBlock(SchemaUpdateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
