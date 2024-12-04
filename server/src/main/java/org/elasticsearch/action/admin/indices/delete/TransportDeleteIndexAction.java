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

package org.elasticsearch.action.admin.indices.delete;

import java.io.IOException;
import java.util.Arrays;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete index action.
 */
public class TransportDeleteIndexAction extends TransportMasterNodeAction<DeleteIndexRequest, AcknowledgedResponse> {

    private final MetadataDeleteIndexService deleteIndexService;

    @Inject
    public TransportDeleteIndexAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      MetadataDeleteIndexService deleteIndexService) {
        super(DeleteIndexAction.NAME, transportService, clusterService, threadPool, DeleteIndexRequest::new);
        this.deleteIndexService = deleteIndexService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        return state.blocks().indicesAllowReleaseResources(IndexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(final DeleteIndexRequest request,
                                   final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        final Index[] concreteIndices = IndexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices.length == 0) {
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }
        String source = "delete-index " + Arrays.toString(request.indices());
        var updateTask = new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.URGENT, request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Index[] concreteIndices = IndexNameExpressionResolver.concreteIndices(currentState, request);
                return deleteIndexService.deleteIndices(currentState, Arrays.asList(concreteIndices));
            }
        };
        clusterService.submitStateUpdateTask(source, updateTask);
    }
}
