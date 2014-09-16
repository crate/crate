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
import org.elasticsearch.search.query.QuerySearchResult;
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
    private final CrateSearchService searchService;
    private final TransportService transportService;

    @Inject
    public TransportQueryShardAction(ClusterService clusterService,
                                     ThreadPool threadPool,
                                     CrateSearchService searchService,
                                     TransportService transportService) {
        this.searchService = searchService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        executor = threadPool.executor(executorName);
        transportService.registerHandler(transportAction, new TransportHandler());
    }

    public void execute(String node, QueryShardRequest request, ActionListener<QuerySearchResult> listener) {
        new AsyncAction(node, request, listener).start();
    }

    private void shardOperation(QueryShardRequest request, ActionListener<QuerySearchResult> listener) {
        try {
            QuerySearchResult querySearchResult = searchService.executeQueryPhase(request);
            listener.onResponse(querySearchResult);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private class AsyncAction {
        private final String nodeId;
        private final QueryShardRequest request;
        private final ClusterState clusterState;
        private final ActionListener<QuerySearchResult> listener;

        private AsyncAction(String node, QueryShardRequest request, ActionListener<QuerySearchResult> listener) {
            this.listener = listener;
            clusterState = clusterService.state();
            this.nodeId = node;
            this.request = request;
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
                        new DefaultTransportResponseHandler<QuerySearchResult>(listener, executorName) {
                            @Override
                            public QuerySearchResult newInstance() {
                                return new QuerySearchResult();
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
            ActionListener<QuerySearchResult> listener = ResponseForwarder.forwardTo(channel);
            shardOperation(request, listener);
        }

        @Override
        public String executor() {
            return executorName;
        }
    }
}
