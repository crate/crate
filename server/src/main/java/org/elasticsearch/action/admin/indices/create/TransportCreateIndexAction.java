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

package org.elasticsearch.action.admin.indices.create;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.metadata.NodeContext;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetadataCreateIndexService createIndexService;
    private final NodeContext nodeContext;

    @Inject
    public TransportCreateIndexAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      MetadataCreateIndexService createIndexService,
                                      NodeContext nodeContext) {
        super(CreateIndexAction.NAME, transportService, clusterService, threadPool, CreateIndexRequest::new);
        this.createIndexService = createIndexService;
        this.nodeContext = nodeContext;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse read(StreamInput in) throws IOException {
        return new CreateIndexResponse(in);
    }

    @Override
    public ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    public void masterOperation(final CreateIndexRequest request,
                                final ClusterState state,
                                final ActionListener<CreateIndexResponse> listener) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final String indexName = request.index();
        final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
            cause, indexName, request.index())
                .ackTimeout(request.timeout())
                .masterNodeTimeout(request.masterNodeTimeout())
                .settings(request.settings())
                .mapping(request.mapping())
                .aliases(request.aliases())
                .waitForActiveShards(request.waitForActiveShards());

        createIndexService.createIndex(
            nodeContext,
            updateRequest,
            null,
            listener.map(response ->
                new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName))
        );
    }

}
