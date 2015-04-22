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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class TransportBulkCreateIndicesAction extends TransportMasterNodeOperationAction<BulkCreateIndicesRequest, BulkCreateIndicesResponse> {

    private final MetaDataCreateIndexService createIndexService;

    @Inject
    protected TransportBulkCreateIndicesAction(Settings settings,
                                               TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               MetaDataCreateIndexService createIndexService,
                                               ActionFilters actionFilters) {
        super(settings, BulkCreateIndicesAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.createIndexService = createIndexService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected BulkCreateIndicesRequest newRequest() {
        return new BulkCreateIndicesRequest();
    }

    @Override
    protected BulkCreateIndicesResponse newResponse() {
        return new BulkCreateIndicesResponse();
    }

    @Override
    protected void masterOperation(final BulkCreateIndicesRequest request, ClusterState state, final ActionListener<BulkCreateIndicesResponse> listener) throws ElasticsearchException {
        final List<CreateIndexResponse> responses = new ArrayList<>(request.requests().size());
        final BulkCreateIndicesResponse finalResponse = new BulkCreateIndicesResponse(responses);

        // create single indices synchronously
        for (CreateIndexRequest createIndexRequest : request.requests()) {
            final String index =  createIndexRequest.indices()[0];
            final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(createIndexRequest, "bulk", index)
                    .ackTimeout(createIndexRequest.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                    .settings(createIndexRequest.settings()).mappings(createIndexRequest.mappings())
                    .aliases(createIndexRequest.aliases()).customs(createIndexRequest.customs());

            PlainActionFuture<ClusterStateUpdateResponse> actionFuture = new PlainActionFuture<>();
            createIndexService.createIndex(updateRequest, actionFuture);
            try {
                ClusterStateUpdateResponse response = actionFuture.actionGet();
                responses.add(new CreateIndexResponse(response.isAcknowledged()));
            } catch (Throwable t) {
                if (t instanceof IndexAlreadyExistsException && request.ignoreExisting()) {
                    finalResponse.addAlreadyExisted(index);
                    logger.trace("[{}] failed to create", t, index);
                } else {
                    logger.debug("[{}] failed to create, abort", t, index);
                    listener.onFailure(t);
                    return;
                }
            }
        }
        listener.onResponse(finalResponse);
    }



    @Override
    protected ClusterBlockException checkBlock(BulkCreateIndicesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }
}
