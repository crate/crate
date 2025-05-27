/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.jobs.kill;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import io.crate.concurrent.MultiActionListener;

public final class TransportKillNodeAction {

    private TransportKillNodeAction() {
    }

    static <T extends TransportRequest> void broadcast(ClusterService clusterService,
                                                       TransportService transportService,
                                                       T request,
                                                       String actionName,
                                                       ActionListener<KillResponse> listener,
                                                       Collection<String> excludedNodeIds) {
        Stream<DiscoveryNode> nodes = StreamSupport.stream(clusterService.state().nodes().spliterator(), false);
        Collection<DiscoveryNode> filteredNodes = nodes.filter(node -> !excludedNodeIds.contains(node.getId())).collect(
            Collectors.toList());

        MultiActionListener<KillResponse, Long, KillResponse> multiListener =
            new MultiActionListener<>(filteredNodes.size(),
                                      () -> 0L,
                                      (state, response) -> state += response.numKilled(),
                                      KillResponse::new,
                                      listener);

        TransportResponseHandler<KillResponse> responseHandler =
            new ActionListenerResponseHandler<>(actionName, multiListener, KillResponse::new);
        for (DiscoveryNode node : filteredNodes) {
            transportService.sendRequest(node, actionName, request, responseHandler);
        }
    }
}
