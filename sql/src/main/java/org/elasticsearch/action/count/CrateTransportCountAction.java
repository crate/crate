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

package org.elasticsearch.action.count;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * a count action that interprets the routing parameter as one single value,
 * no splitting by comma like the TransportCountAction does
 * <p>
 * delegates everything but doExecute to TransportCountAction
 */
public class CrateTransportCountAction extends TransportBroadcastOperationAction<CountRequest, CountResponse, ShardCountRequest, ShardCountResponse> {

    private final TransportCountAction transportCountAction;

    @Inject
    public CrateTransportCountAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                ActionFilters actionFilters, TransportCountAction transportCountAction) {
        super(settings, CrateCountAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.transportCountAction = transportCountAction;
    }

    @Override
    protected void doExecute(CountRequest request, ActionListener<CountResponse> listener) {
        // copied from TransportCountAction
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    @Override
    protected String executor() {
        // copied from TransportCountAction
        return ThreadPool.Names.SEARCH;
    }

    @Override
    protected CountRequest newRequest() {
        return transportCountAction.newRequest();
    }

    @Override
    protected ShardCountRequest newShardRequest() {
        return transportCountAction.newShardRequest();
    }

    @Override
    protected ShardCountRequest newShardRequest(int numShards, ShardRouting shard, CountRequest request) {
        return transportCountAction.newShardRequest(numShards, shard, request);
    }

    @Override
    protected ShardCountResponse newShardResponse() {
        return transportCountAction.newShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, CountRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(
                request.routing() == null ? ImmutableSet.<String>of() : ImmutableSet.of(request.routing()),
                request.indices()
        );
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, CountRequest request) {
        return transportCountAction.checkGlobalBlock(state, request);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, CountRequest countRequest, String[] concreteIndices) {
        return transportCountAction.checkRequestBlock(state, countRequest, concreteIndices);
    }

    @Override
    protected CountResponse newResponse(CountRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        return transportCountAction.newResponse(request, shardsResponses, clusterState);
    }

    @Override
    protected ShardCountResponse shardOperation(ShardCountRequest request) throws ElasticsearchException {
        return transportCountAction.shardOperation(request);
    }
}
