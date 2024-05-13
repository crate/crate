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

import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
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
    CompletableFuture<IndicesStatsResponse> stats(IndicesStatsRequest request);

    /**
     * Indices recoveries
     */
    CompletableFuture<RecoveryResponse> recoveries(RecoveryRequest request);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request The create index request
     * @return The result future
     */
    CompletableFuture<CreateIndexResponse> create(CreateIndexRequest request);

    /**
     * Deletes an index based on the index name.
     *
     * @param request The delete index request
     * @return The result future
     */
    CompletableFuture<AcknowledgedResponse> delete(DeleteIndexRequest request);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request The refresh request
     * @return The result future
     */
    CompletableFuture<RefreshResponse> refresh(RefreshRequest request);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request The create mapping request
     * @return A result future
     */
    CompletableFuture<AcknowledgedResponse> putMapping(PutMappingRequest request);

    /**
     * Update indices settings.
     */
    CompletableFuture<AcknowledgedResponse> updateSettings(UpdateSettingsRequest request);

    /**
     * Gets index template.
     */
    CompletableFuture<GetIndexTemplatesResponse> getTemplates(GetIndexTemplatesRequest request);
}
