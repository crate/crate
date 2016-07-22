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

package io.crate.executor.transport;

import io.crate.Build;
import io.crate.Version;
import io.crate.operation.reference.sys.node.DiscoveryNodeContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportNodeStatsAction implements NodeAction<NodeStatsRequest, NodeStatsResponse> {

    public static final String ACTION_NAME = "crate/sql/sys/nodes";
    private static final String EXECUTOR = ThreadPool.Names.PERCOLATE;

    private final ClusterService clusterService;
    private final JvmService jvmService;
    private final Transports transports;

    @Inject
    public TransportNodeStatsAction(TransportService transportService,
                                    ClusterService clusterService,
                                    JvmService jvmService,
                                    Transports transports) {
        this.clusterService = clusterService;
        this.jvmService = jvmService;
        this.transports = transports;
        transportService.registerRequestHandler(ACTION_NAME,
                NodeStatsRequest.class,
                EXECUTOR,
                new NodeActionRequestHandler<NodeStatsRequest, NodeStatsResponse>(this) {
                }
        );
    }

    public void execute(final String nodeId,
                        final NodeStatsRequest request,
                        final ActionListener<NodeStatsResponse> listener,
                        final TimeValue timeout) {
        TransportRequestOptions options = TransportRequestOptions.builder()
                .withTimeout(timeout)
                .build();

        transports.sendRequest(ACTION_NAME, nodeId, request, listener,
                new DefaultTransportResponseHandler<NodeStatsResponse>(listener) {
                    @Override
                    public NodeStatsResponse newInstance() {
                        return new NodeStatsResponse();
                    }
                }, options);
    }

    @Override
    public void nodeOperation(NodeStatsRequest request, ActionListener<NodeStatsResponse> listener) {
        DiscoveryNodeContext context = new DiscoveryNodeContext();
        ClusterState state = clusterService.state();
        context.id = clusterService.localNode().id();
        context.name = clusterService.localNode().name();
        context.version = Version.CURRENT;
        context.build = Build.CURRENT;
        context.hostname = clusterService.localNode().getHostName();
        context.jvmStats = jvmService.stats();
        NodeStatsResponse response = new NodeStatsResponse(context);
        listener.onResponse(response);
    }
}
