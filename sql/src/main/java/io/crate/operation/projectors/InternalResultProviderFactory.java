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

package io.crate.operation.projectors;

import com.google.common.collect.Lists;
import io.crate.Streamer;
import io.crate.executor.transport.distributed.BroadcastDistributingDownstream;
import io.crate.executor.transport.distributed.ModuloDistributingDownstream;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.executor.transport.distributed.TransportDistributedResultAction;
import io.crate.operation.NodeOperation;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

@Singleton
public class InternalResultProviderFactory implements ResultProviderFactory {

    private final ClusterService clusterService;
    private final TransportDistributedResultAction transportDistributedResultAction;
    private final Settings settings;

    @Inject
    public InternalResultProviderFactory(ClusterService clusterService,
                                         TransportDistributedResultAction transportDistributedResultAction,
                                         Settings settings) {
        this.clusterService = clusterService;
        this.transportDistributedResultAction = transportDistributedResultAction;
        this.settings = settings;
    }

    public ResultProvider createDownstream(NodeOperation nodeOperation, UUID jobId) {
        Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());

        if (nodeOperation.downstreamExecutionPhaseId() == ExecutionPhase.NO_EXECUTION_PHASE ||
                ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())) {
            return new SingleBucketBuilder(streamers);
        } else {
            assert nodeOperation.downstreamNodes().size() > 0 : "must have at least one downstream";

            // TODO: set bucketIdx properly
            ArrayList<String> server = Lists.newArrayList(nodeOperation.executionPhase().executionNodes());
            Collections.sort(server);
            int bucketIdx = Math.max(server.indexOf(clusterService.localNode().id()), 0);

            if (nodeOperation.downstreamNodes().size() == 1) {
                // using modulo based distribution does not make sense if distributing to 1 node only
                // lets use the broadcast distribution which simply passes every bucket to every node
                return new BroadcastDistributingDownstream(
                        jobId,
                        nodeOperation.downstreamExecutionPhaseId(),
                        nodeOperation.downstreamExecutionPhaseInputId(),
                        bucketIdx,
                        nodeOperation.downstreamNodes(),
                        transportDistributedResultAction,
                        streamers,
                        settings
                );
            }

            return new ModuloDistributingDownstream(
                    jobId,
                    nodeOperation.downstreamExecutionPhaseId(),
                    nodeOperation.downstreamExecutionPhaseInputId(),
                    bucketIdx,
                    nodeOperation.downstreamNodes(),
                    transportDistributedResultAction,
                    streamers,
                    settings
            );
        }
    }
}
