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

import io.crate.Streamer;
import io.crate.data.RowConsumer;
import io.crate.executor.transport.distributed.BroadcastingBucketBuilder;
import io.crate.executor.transport.distributed.DistributingConsumer;
import io.crate.executor.transport.distributed.ModuloBucketBuilder;
import io.crate.executor.transport.distributed.MultiBucketBuilder;
import io.crate.executor.transport.distributed.TransportDistributedResultAction;
import io.crate.operation.NodeOperation;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Executor;

@Singleton
public class DistributingDownstreamFactory extends AbstractComponent {

    private static final String RESPONSE_EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final ClusterService clusterService;
    private final Executor responseExecutor;
    private final TransportDistributedResultAction transportDistributedResultAction;
    private final Logger distributingDownstreamLogger;

    @Inject
    public DistributingDownstreamFactory(Settings settings,
                                         ClusterService clusterService,
                                         ThreadPool threadPool,
                                         TransportDistributedResultAction transportDistributedResultAction) {
        super(settings);
        this.clusterService = clusterService;
        this.responseExecutor = threadPool.executor(RESPONSE_EXECUTOR_NAME);
        this.transportDistributedResultAction = transportDistributedResultAction;
        distributingDownstreamLogger = Loggers.getLogger(DistributingConsumer.class, settings);
    }

    public RowConsumer create(NodeOperation nodeOperation,
                              DistributionInfo distributionInfo,
                              UUID jobId,
                              int pageSize) {
        Streamer<?>[] streamers = StreamerVisitor.streamersFromOutputs(nodeOperation.executionPhase());
        assert !ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())
            : "trying to build a DistributingDownstream but nodeOperation has a directResponse downstream";
        assert nodeOperation.downstreamNodes().size() > 0 : "must have at least one downstream";

        byte phaseInputId = nodeOperation.downstreamExecutionPhaseInputId();
        int bucketIdx = getBucketIdx(nodeOperation.executionPhase().nodeIds(), phaseInputId);

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
            responseExecutor,
            jobId,
            multiBucketBuilder,
            nodeOperation.downstreamExecutionPhaseId(),
            phaseInputId,
            bucketIdx,
            nodeOperation.downstreamNodes(),
            transportDistributedResultAction,
            streamers,
            pageSize
        );
    }

    /**
     * @return bucketIdx (= phaseInputID (8bit) | idx of localNode in nodeIds (24bit) )
     *
     * <p>
     * This needs to be deterministic across all nodes involved in a query execution.
     *
     * Downstreams assume that each upstream uses a *different* bucketIdx,
     * but they don't care which node ends up with which bucketIdx.
     * </p>
     *
     * See {@link io.crate.jobs.PageBucketReceiver}
     */
    private int getBucketIdx(Collection<String> nodeIds, byte phaseInputId) {
        ArrayList<String> server = new ArrayList<>(nodeIds);
        server.sort(null);
        int nodeId = Math.max(server.indexOf(clusterService.localNode().getId()), 0);
        return nodeId | (phaseInputId << 24);
    }
}
