/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.executor.transport.distributed.DistributingDownstream;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.util.List;

/**
 * handling distributing collect requests
 * collected data is distributed to downstream nodes that further merge/reduce their data
 */
@Singleton
public class DistributingCollectOperation extends MapSideDataCollectOperation<DistributingDownstream> {

    private final TransportService transportService;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public DistributingCollectOperation(ClusterService clusterService,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        Functions functions,
                                        ReferenceResolver referenceResolver,
                                        IndicesService indicesService,
                                        ThreadPool threadPool,
                                        TransportService transportService,
                                        StreamerVisitor streamerVisitor,
                                        CollectServiceResolver collectServiceResolver,
                                        JobContextService jobContextService,
                                        CrateCircuitBreakerService breakerService) {
        super(clusterService, settings, transportActionProvider,
                functions, referenceResolver, indicesService,
                threadPool, collectServiceResolver, streamerVisitor, jobContextService);
        this.transportService = transportService;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
    }

    private List<DiscoveryNode> toDiscoveryNodes(List<String> nodeIds) {
        final DiscoveryNodes discoveryNodes = clusterService.state().nodes();
        return Lists.transform(nodeIds, new Function<String, DiscoveryNode>() {
            @Nullable
            @Override
            public DiscoveryNode apply(@Nullable String input) {
                assert input != null;
                return discoveryNodes.get(input);
            }
        });
    }


    @Override
    public DistributingDownstream createDownstream(CollectNode node) {
        assert node.jobId().isPresent();
        Streamer<?>[] streamers = streamerVisitor.processPlanNode(
                (PlanNode) node, new RamAccountingContext("dummy", circuitBreaker)).outputStreamers();
        return new DistributingDownstream(
                node.jobId().get(),
                toDiscoveryNodes(node.downstreamNodes()),
                transportService,
                streamers
        );
    }
}
