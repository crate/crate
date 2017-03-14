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
import io.crate.data.BatchConsumer;
import io.crate.executor.transport.distributed.*;
import io.crate.operation.NodeOperation;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

@Singleton
public class DistributingDownstreamFactory extends AbstractComponent {

    private final ClusterService clusterService;
    private final TransportDistributedResultAction transportDistributedResultAction;
    private final ESLogger distributingDownstreamLogger;

    @Inject
    public DistributingDownstreamFactory(Settings settings,
                                         ClusterService clusterService,
                                         TransportDistributedResultAction transportDistributedResultAction) {
        super(settings);
        this.clusterService = clusterService;
        this.transportDistributedResultAction = transportDistributedResultAction;
        distributingDownstreamLogger = Loggers.getLogger(DistributingConsumer.class, settings);
    }

    public BatchConsumer create(NodeOperation nodeOperation,
                                DistributionInfo distributionInfo,
                                UUID jobId,
                                int pageSize) {
        Streamer<?>[] streamers = StreamerVisitor.streamersFromOutputs(nodeOperation.executionPhase());
        assert !ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())
            : "trying to build a DistributingDownstream but nodeOperation has a directResponse downstream";
        assert nodeOperation.downstreamNodes().size() > 0 : "must have at least one downstream";

        // TODO: set bucketIdx properly
        ArrayList<String> server = Lists.newArrayList(nodeOperation.executionPhase().nodeIds());
        Collections.sort(server);
        int bucketIdx = Math.max(server.indexOf(clusterService.localNode().getId()), 0);

        MultiBucketBuilder multiBucketBuilder;
        switch (distributionInfo.distributionType()) {
            case MODULO:
                if (nodeOperation.downstreamNodes().size() == 1) {
                    multiBucketBuilder = new BroadcastingBucketBuilder(streamers, nodeOperation.downstreamNodes().size());
                } else {
                    multiBucketBuilder = new ModuloBucketBuilder(streamers,
                        nodeOperation.downstreamNodes().size(), distributionInfo.distributeByColumn());
                }
                break;
            case BROADCAST:
                multiBucketBuilder = new BroadcastingBucketBuilder(streamers, nodeOperation.downstreamNodes().size());
                break;
            default:
                throw new UnsupportedOperationException("Can't handle distributionInfo: " + distributionInfo);
        }

        return new DistributingConsumer(
            distributingDownstreamLogger,
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
