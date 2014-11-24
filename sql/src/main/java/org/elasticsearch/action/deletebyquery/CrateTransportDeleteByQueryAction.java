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

package org.elasticsearch.action.deletebyquery;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.replication.TransportIndicesReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * a copied delete by query action that interprets the routing parameter as one single value,
 * no splitting by comma
 *
 * Everything but {@linkplain #resolveRouting(org.elasticsearch.cluster.ClusterState, DeleteByQueryRequest)} is copied from
 * {@linkplain org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction}.
 */
public class CrateTransportDeleteByQueryAction extends TransportIndicesReplicationOperationAction<DeleteByQueryRequest, DeleteByQueryResponse, IndexDeleteByQueryRequest, IndexDeleteByQueryResponse, ShardDeleteByQueryRequest, ShardDeleteByQueryRequest, ShardDeleteByQueryResponse> {

    private final DestructiveOperations destructiveOperations;

    @Inject
    public CrateTransportDeleteByQueryAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        ThreadPool threadPool, TransportIndexDeleteByQueryAction indexDeleteByQueryAction,
                                        NodeSettingsService nodeSettingsService, ActionFilters actionFilters) {
        super(settings, CrateDeleteByQueryAction.NAME, transportService, clusterService, threadPool, indexDeleteByQueryAction, actionFilters);
        this.destructiveOperations = new DestructiveOperations(logger, settings, nodeSettingsService);
    }

    @Override
    protected void doExecute(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(request, listener);
    }

    @Override
    protected DeleteByQueryRequest newRequestInstance() {
        return new DeleteByQueryRequest();
    }

    @Override
    protected DeleteByQueryResponse newResponseInstance(DeleteByQueryRequest request, AtomicReferenceArray indexResponses) {
        DeleteByQueryResponse response = new DeleteByQueryResponse();
        for (int i = 0; i < indexResponses.length(); i++) {
            IndexDeleteByQueryResponse indexResponse = (IndexDeleteByQueryResponse) indexResponses.get(i);
            if (indexResponse != null) {
                response.getIndices().put(indexResponse.getIndex(), indexResponse);
            }
        }
        return response;
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DeleteByQueryRequest replicationPingRequest) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, DeleteByQueryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, concreteIndices);
    }

    @Override
    protected IndexDeleteByQueryRequest newIndexRequestInstance(DeleteByQueryRequest request, String index, Set<String> routing, long startTimeInMillis) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(index, request.indices());
        return new IndexDeleteByQueryRequest(request, index, routing, filteringAliases, startTimeInMillis);
    }

    @Override
    protected Map<String, Set<String>> resolveRouting(ClusterState clusterState,
                                                      DeleteByQueryRequest request) throws ElasticsearchException {
        return clusterState.metaData().resolveSearchRouting(
                request.routing() == null ? ImmutableSet.<String>of() : ImmutableSet.of(request.routing()),
                request.indices()
        );
    }
}
