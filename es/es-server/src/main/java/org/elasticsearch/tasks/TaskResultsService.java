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
package org.elasticsearch.tasks;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Service that can store task results.
 */
public class TaskResultsService extends AbstractComponent {

    public static final String TASK_INDEX = ".tasks";

    public static final String TASK_TYPE = "task";

    public static final String TASK_RESULT_INDEX_MAPPING_FILE = "task-index-mapping.json";

    public static final String TASK_RESULT_MAPPING_VERSION_META_FIELD = "version";

    public static final int TASK_RESULT_MAPPING_VERSION = 2;

    private final Client client;

    private final ClusterService clusterService;

    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TaskResultsService(Settings settings, Client client, ClusterService clusterService,
                              TransportCreateIndexAction createIndexAction) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.createIndexAction = createIndexAction;
    }

    public void storeResult(TaskResult taskResult, ActionListener<Void> listener) {

        ClusterState state = clusterService.state();

        if (state.routingTable().hasIndex(TASK_INDEX) == false) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.settings(taskResultIndexSettings());
            createIndexRequest.index(TASK_INDEX);
            createIndexRequest.mapping(TASK_TYPE, taskResultIndexMapping(), XContentType.JSON);
            createIndexRequest.cause("auto(task api)");

            createIndexAction.execute(null, createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    doStoreResult(taskResult, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            doStoreResult(taskResult, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            IndexMetaData metaData = state.getMetaData().index(TASK_INDEX);
            if (getTaskResultMappingVersion(metaData) < TASK_RESULT_MAPPING_VERSION) {
                // The index already exists but doesn't have our mapping
                client.admin().indices().preparePutMapping(TASK_INDEX).setType(TASK_TYPE)
                    .setSource(taskResultIndexMapping(), XContentType.JSON)
                    .execute(new ActionListener<AcknowledgedResponse>() {
                                 @Override
                                 public void onResponse(AcknowledgedResponse putMappingResponse) {
                                     doStoreResult(taskResult, listener);
                                 }

                                 @Override
                                 public void onFailure(Exception e) {
                                     listener.onFailure(e);
                                 }
                             }
                    );
            } else {
                doStoreResult(taskResult, listener);
            }
        }
    }

    private int getTaskResultMappingVersion(IndexMetaData metaData) {
        MappingMetaData mappingMetaData = metaData.getMappings().get(TASK_TYPE);
        if (mappingMetaData == null) {
            return 0;
        }
        @SuppressWarnings("unchecked") Map<String, Object> meta = (Map<String, Object>) mappingMetaData.sourceAsMap().get("_meta");
        if (meta == null || meta.containsKey(TASK_RESULT_MAPPING_VERSION_META_FIELD) == false) {
            return 1; // The mapping was created before meta field was introduced
        }
        return (int) meta.get(TASK_RESULT_MAPPING_VERSION_META_FIELD);
    }

    private void doStoreResult(TaskResult taskResult, ActionListener<Void> listener) {
        IndexRequestBuilder index = client.prepareIndex(TASK_INDEX, TASK_TYPE, taskResult.getTask().getTaskId().toString());
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            taskResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
            index.setSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchException("Couldn't convert task result to XContent for [{}]", e, taskResult.getTask());
        }
        index.execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Settings taskResultIndexSettings() {
        return Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetaData.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }

    public String taskResultIndexMapping() {
        try (InputStream is = getClass().getResourceAsStream(TASK_RESULT_INDEX_MAPPING_FILE)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toString(StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage(
                    "failed to create tasks results index template [{}]", TASK_RESULT_INDEX_MAPPING_FILE), e);
            throw new IllegalStateException("failed to create tasks results index template [" + TASK_RESULT_INDEX_MAPPING_FILE + "]", e);
        }

    }
}
