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
import io.crate.executor.transport.distributed.*;
import io.crate.operation.NodeOperation;
import io.crate.operation.RowDownstream;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

@Singleton
public class InternalRowDownstreamFactory implements RowDownstreamFactory {

    private final ClusterService clusterService;
    private final TransportDistributedResultAction transportDistributedResultAction;

    @Inject
    public InternalRowDownstreamFactory(ClusterService clusterService,
                                        TransportDistributedResultAction transportDistributedResultAction) {
        this.clusterService = clusterService;
        this.transportDistributedResultAction = transportDistributedResultAction;
    }

    public RowDownstream createDownstream(NodeOperation nodeOperation, UUID jobId, int pageSize) {
        Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());
        assert !ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes());
        assert nodeOperation.downstreamNodes().size() > 0 : "must have at least one downstream";

        // TODO: set bucketIdx properly
        ArrayList<String> server = Lists.newArrayList(nodeOperation.executionPhase().executionNodes());
        Collections.sort(server);
        int bucketIdx = Math.max(server.indexOf(clusterService.localNode().id()), 0);

        MultiBucketBuilder multiBucketBuilder;
        if (nodeOperation.downstreamNodes().size() == 1) {
            // using modulo based distribution does not make sense if distributing to 1 node only
            // lets use the broadcast distribution which simply passes every bucket to every node

            multiBucketBuilder = new BroadcastingBucketBuilder(streamers, nodeOperation.downstreamNodes().size());
        } else {
            multiBucketBuilder = new ModuloBucketBuilder(streamers, nodeOperation.downstreamNodes().size());
        }

        return new DistributingDownstream(
                jobId,
                multiBucketBuilder,
                nodeOperation.downstreamExecutionPhaseId(),
                nodeOperation.downstreamExecutionPhaseInputId(),
                bucketIdx,
                nodeOperation.downstreamNodes(),
                transportDistributedResultAction,
                streamers,
                pageSize
        );

    }
}
