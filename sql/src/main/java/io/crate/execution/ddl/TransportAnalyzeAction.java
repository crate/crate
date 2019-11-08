/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl;

import io.crate.execution.support.MultiActionListener;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.TableStatsService;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

@Singleton
public final class TransportAnalyzeAction implements NodeAction<AnalyzeRequest, AcknowledgedResponse> {

    private static final String NAME = "internal:crate:sql/analyze";
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final TableStatsService tableStatsService;

    @Inject
    public TransportAnalyzeAction(TransportService transportService,
                                  ClusterService clusterService,
                                  TableStatsService tableStatsService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.tableStatsService = tableStatsService;
        transportService.registerRequestHandler(
            NAME,
            AnalyzeRequest::new,
            ThreadPool.Names.SAME, // goes async right away
            new NodeActionRequestHandler<>(this)
        );
    }

    @Override
    public CompletableFuture<AcknowledgedResponse> nodeOperation(AnalyzeRequest request) {
        return tableStatsService.updateStats().thenApply(aVoid -> new AcknowledgedResponse(true));
    }

    public void broadcast(AnalyzeRequest analyzeRequest, OneRowActionListener<AcknowledgedResponse> listener) {
        DiscoveryNodes nodes = clusterService.state().getNodes();

        Function<List<AcknowledgedResponse>, AcknowledgedResponse> mergeResponses =
            requests -> new AcknowledgedResponse(requests.stream().allMatch(AcknowledgedResponse::isAcknowledged));
        MultiActionListener<AcknowledgedResponse, ?, AcknowledgedResponse> multiListener = new MultiActionListener<>(
            nodes.getSize(),
            Collectors.collectingAndThen(Collectors.toList(), mergeResponses),
            listener
        );
        ActionListenerResponseHandler<AcknowledgedResponse> responseHandler = new ActionListenerResponseHandler<>(
            multiListener,
            AcknowledgedResponse::new,
            ThreadPool.Names.SAME
        );
        for (DiscoveryNode node : nodes) {
            transportService.sendRequest(node, NAME, analyzeRequest, responseHandler);
        }
    }
}
