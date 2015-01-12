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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public class TransportQueryShardAction {

    private final String queryTransportAction = "crate/sql/shard/query";
    private final String queryScrollTransportAction = "crate/sql/shard/query/scroll";
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
        transportService.registerHandler(queryTransportAction, new QueryTransportHandler());
        transportService.registerHandler(queryScrollTransportAction, new ScrollTransportHandler());
    }

    public void executeLocalOrViaTransport(String node,
                                          Runnable localRunnable,
                                          ActionRequest request,
                                          String transportActionName,
                                          TransportResponseHandler<?> responseHandler) {
        ClusterState clusterState = clusterService.state();
        if (node.equals("_local") || node.equals(clusterState.nodes().localNodeId())) {
            executor.execute(localRunnable);
        } else {
            transportService.sendRequest(
                    clusterState.nodes().get(node),
                    transportActionName,
                    request,
                    responseHandler
            );
        }

    }

    public void executeQuery(String node, final QueryShardRequest request, final ActionListener<QuerySearchResult> listener) {
        Runnable localRunnable = new Runnable() {
            @Override
            public void run() {
                executeQueryOnShard(request, listener);
            }
        };
        TransportResponseHandler<?> responseHandler = new DefaultTransportResponseHandler<QuerySearchResult>(listener, executorName) {
            @Override
            public QuerySearchResult newInstance() {
                return new QuerySearchResult();
            }
        };
        executeLocalOrViaTransport(node, localRunnable, request, queryTransportAction, responseHandler);
    }

    private void executeQueryOnShard(QueryShardRequest request, ActionListener<QuerySearchResult> listener) {
        try {
            QuerySearchResult querySearchResult = searchService.executeQueryPhase(request);
            listener.onResponse(querySearchResult);
        } catch (Throwable e) {
            listener.onFailure(e);
        }
    }

    private class QueryTransportHandler extends BaseTransportRequestHandler<QueryShardRequest> {

        @Override
        public QueryShardRequest newInstance() {
            return new QueryShardRequest();
        }

        @Override
        public void messageReceived(QueryShardRequest request, TransportChannel channel) throws Exception {
            ActionListener<QuerySearchResult> listener = ResponseForwarder.forwardTo(channel);
            executeQueryOnShard(request, listener);
        }

        @Override
        public String executor() {
            return executorName;
        }
    }

    public void executeScroll(String node, final QueryShardScrollRequest request, final ActionListener<ScrollQueryFetchSearchResult> listener) {
        Runnable localRunnable = new Runnable() {
            @Override
            public void run() {
                executeScrollOnShard(request, listener);
            }
        };
        TransportResponseHandler<?> responseHandler = new DefaultTransportResponseHandler<ScrollQueryFetchSearchResult>(listener, executorName) {
            @Override
            public ScrollQueryFetchSearchResult newInstance() {
                return new ScrollQueryFetchSearchResult();
            }
        };
        executeLocalOrViaTransport(node, localRunnable, request, queryScrollTransportAction, responseHandler);


    }

    public void executeScrollOnShard(QueryShardScrollRequest request, ActionListener<ScrollQueryFetchSearchResult> listener) {
        try {
            ScrollQueryFetchSearchResult result = searchService.executeScrollPhase(request);
            listener.onResponse(result);
        } catch (Throwable e) {
            listener.onFailure(e);
        }
    }

    private class ScrollTransportHandler extends BaseTransportRequestHandler<QueryShardScrollRequest> {

        @Override
        public QueryShardScrollRequest newInstance() {
            return new QueryShardScrollRequest();
        }

        @Override
        public void messageReceived(QueryShardScrollRequest request, TransportChannel channel) throws Exception {
            ActionListener<ScrollQueryFetchSearchResult> listener = ResponseForwarder.forwardTo(channel);
            executeScrollOnShard(request, listener);
        }

        @Override
        public String executor() {
            return executorName;
        }
    }
}
