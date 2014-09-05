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

package io.crate.action.sql.query;

import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.ResponseForwarder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public class TransportQueryShardAction {

    private final String transportAction = "crate/sql/shard/query";
    private final static String executorName = ThreadPool.Names.SEARCH;
    private final ClusterService clusterService;
    private final Executor executor;
    private final TransportService transportService;

    @Inject
    public TransportQueryShardAction(ClusterService clusterService,
                                     ThreadPool threadPool,
                                     TransportService transportService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        executor = threadPool.executor(executorName);
        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(String node, QueryShardRequest request, ActionListener<QueryShardResponse> listener) {
        new AsyncAction(node, request, listener).start();
    }

    private void shardOperation(QueryShardRequest request, ActionListener<QueryShardResponse> listener) {
        // do stuff
        listener.onResponse(new QueryShardResponse());
    }

    private class AsyncAction {
        private final String nodeId;
        private final QueryShardRequest request;
        private final ActionListener<QueryShardResponse> listener;
        private final ClusterState clusterState;

        private AsyncAction(String node, QueryShardRequest request, ActionListener<QueryShardResponse> listener) {
            clusterState = clusterService.state();
            this.nodeId = node;
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            if (nodeId.equals("_local") || nodeId.equals(clusterState.nodes().localNodeId())) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        shardOperation(request, listener);
                    }
                });
            } else {
                transportService.sendRequest(
                        clusterState.nodes().get(nodeId),
                        transportAction,
                        request,
                        new DefaultTransportResponseHandler<QueryShardResponse>(listener, executorName) {
                            @Override
                            public QueryShardResponse newInstance() {
                                return new QueryShardResponse();
                            }
                        }
                );
            }
        }
    }


    private class TransportHandler extends BaseTransportRequestHandler<QueryShardRequest> {

        @Override
        public QueryShardRequest newInstance() {
            return new QueryShardRequest();
        }

        @Override
        public void messageReceived(QueryShardRequest request, TransportChannel channel) throws Exception {
            ActionListener<QueryShardResponse> listener = ResponseForwarder.forwardTo(channel);
            shardOperation(request, listener);
        }

        @Override
        public String executor() {
            return executorName;
        }
    }
}
