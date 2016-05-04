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

package io.crate.executor.transport.kill;

import io.crate.executor.MultiActionListener;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.jobs.JobContextService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportKillAllNodeAction implements NodeAction<KillAllRequest, KillResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/kill_all";


    private final JobContextService jobContextService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportKillAllNodeAction(JobContextService jobContextService,
                                      ClusterService clusterService,
                                      TransportService transportService) {
        this.jobContextService = jobContextService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerRequestHandler(TRANSPORT_ACTION,
                KillAllRequest.class,
                ThreadPool.Names.GENERIC,
                new NodeActionRequestHandler<KillAllRequest, KillResponse>(this) { });
    }

    public void broadcast(KillAllRequest request, ActionListener<KillResponse> listener) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        listener = new MultiActionListener<>(nodes.size(), KillResponse.MERGE_FUNCTION, listener);
        DefaultTransportResponseHandler<KillResponse> responseHandler = new DefaultTransportResponseHandler<KillResponse>(listener) {
            @Override
            public KillResponse newInstance() {
                return new KillResponse(0);
            }
        };
        for (DiscoveryNode node : nodes) {
            transportService.sendRequest(node, TRANSPORT_ACTION, request, responseHandler);
        }
    }

    @Override
    public void nodeOperation(KillAllRequest request, ActionListener<KillResponse> listener) {
        try {
            listener.onResponse(new KillResponse(jobContextService.killAll()));
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }

}
