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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#indices()
 */
public interface IndicesAdminClient extends ElasticsearchClient {

    /**
     * Indices stats.
     */
    ActionFuture<IndicesStatsResponse> stats(IndicesStatsRequest request);

    /**
     * Indices stats.
     */
    void stats(IndicesStatsRequest request, ActionListener<IndicesStatsResponse> listener);

    /**
     * Indices stats.
     */
    IndicesStatsRequestBuilder prepareStats(String... indices);

    /**
     * Indices recoveries
     */
    ActionFuture<RecoveryResponse> recoveries(RecoveryRequest request);

    /**
     *Indices recoveries
     */
    void recoveries(RecoveryRequest request, ActionListener<RecoveryResponse> listener);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request The create index request
     * @return The result future
     * @see org.elasticsearch.client.Requests#createIndexRequest(String)
     */
    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request  The create index request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#createIndexRequest(String)
     */
    void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param index The index name to create
     */
    CreateIndexRequestBuilder prepareCreate(String index);

    /**
     * Deletes an index based on the index name.
     *
     * @param request The delete index request
     * @return The result future
     * @see org.elasticsearch.client.Requests#deleteIndexRequest(String)
     */
    ActionFuture<AcknowledgedResponse> delete(DeleteIndexRequest request);

    /**
     * Deletes an index based on the index name.
     *
     * @param request  The delete index request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#deleteIndexRequest(String)
     */
    void delete(DeleteIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes an index based on the index name.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    DeleteIndexRequestBuilder prepareDelete(String... indices);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request The refresh request
     * @return The result future
     * @see org.elasticsearch.client.Requests#refreshRequest(String...)
     */
    ActionFuture<RefreshResponse> refresh(RefreshRequest request);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request  The refresh request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#refreshRequest(String...)
     */
    void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     */
    RefreshRequestBuilder prepareRefresh(String... indices);

    /**
     * Explicitly force merge one or more indices into a the number of segments.
     *
     * @param request The optimize request
     * @return A result future
     * @see org.elasticsearch.client.Requests#forceMergeRequest(String...)
     */
    ActionFuture<ForceMergeResponse> forceMerge(ForceMergeRequest request);

    /**
     * Explicitly force merge one or more indices into a the number of segments.
     *
     * @param request  The force merge request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#forceMergeRequest(String...)
     */
    void forceMerge(ForceMergeRequest request, ActionListener<ForceMergeResponse> listener);

    /**
     * Explicitly force merge one or more indices into a the number of segments.
     */
    ForceMergeRequestBuilder prepareForceMerge(String... indices);

    /**
     * Explicitly upgrade one or more indices
     *
     * @param request The upgrade request
     * @return A result future
     * @see org.elasticsearch.client.Requests#upgradeRequest(String...)
     */
    ActionFuture<UpgradeResponse> upgrade(UpgradeRequest request);

    /**
     * Explicitly upgrade one or more indices
     *
     * @param request  The upgrade request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#upgradeRequest(String...)
     */
    void upgrade(UpgradeRequest request, ActionListener<UpgradeResponse> listener);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request The create mapping request
     * @return A result future
     * @see org.elasticsearch.client.Requests#putMappingRequest(String...)
     */
    ActionFuture<AcknowledgedResponse> putMapping(PutMappingRequest request);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request  The create mapping request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#putMappingRequest(String...)
     */
    void putMapping(PutMappingRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Add mapping definition for a type into one or more indices.
     */
    PutMappingRequestBuilder preparePutMapping(String... indices);

    /**
     * Updates settings of one or more indices.
     *
     * @param request the update settings request
     * @return The result future
     */
    ActionFuture<AcknowledgedResponse> updateSettings(UpdateSettingsRequest request);

    /**
     * Updates settings of one or more indices.
     *
     * @param request  the update settings request
     * @param listener A listener to be notified with the response
     */
    void updateSettings(UpdateSettingsRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Update indices settings.
     */
    UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices);

    /**
     * Puts an index template.
     */
    ActionFuture<AcknowledgedResponse> putTemplate(PutIndexTemplateRequest request);

    /**
     * Puts an index template.
     */
    void putTemplate(PutIndexTemplateRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Puts an index template.
     *
     * @param name The name of the template.
     */
    PutIndexTemplateRequestBuilder preparePutTemplate(String name);

    /**
     * Deletes an index template.
     */
    void deleteTemplate(DeleteIndexTemplateRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes an index template.
     *
     * @param name The name of the template.
     */
    DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name);

    /**
     * Gets index template.
     */
    ActionFuture<GetIndexTemplatesResponse> getTemplates(GetIndexTemplatesRequest request);

    /**
     * Gets an index template.
     */
    void getTemplates(GetIndexTemplatesRequest request, ActionListener<GetIndexTemplatesResponse> listener);

    /**
     * Gets an index template (optional).
     */
    GetIndexTemplatesRequestBuilder prepareGetTemplates(String... name);
}
