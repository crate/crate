/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Executor;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.Streamer;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.phases.ExecutionPhases;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.NodeRequest;
import io.crate.planner.distribution.DistributionInfo;

@Singleton
public class DistributingConsumerFactory {

    private static final String RESPONSE_EXECUTOR_NAME = ThreadPool.Names.SEARCH;

    private final ClusterService clusterService;
    private final Executor responseExecutor;
    private final ActionExecutor<NodeRequest<DistributedResultRequest>, DistributedResultResponse> distributedResultAction;

    @Inject
    public DistributingConsumerFactory(ClusterService clusterService,
                                       ThreadPool threadPool,
                                       Node node) {
        this.clusterService = clusterService;
        this.responseExecutor = threadPool.executor(RESPONSE_EXECUTOR_NAME);
        this.distributedResultAction = req -> node.client().execute(DistributedResultAction.INSTANCE, req);
    }

    public RowConsumer create(NodeOperation nodeOperation,
                              RamAccounting ramAccounting,
                              DistributionInfo distributionInfo,
                              UUID jobId,
                              int pageSize) {
        Streamer<?>[] streamers = nodeOperation.executionPhase().getStreamers();
        assert !ExecutionPhases.hasDirectResponseDownstream(nodeOperation.downstreamNodes())
            : "trying to build a DistributingDownstream but nodeOperation has a directResponse downstream";
        assert nodeOperation.downstreamNodes().size() > 0 : "must have at least one downstream";

        byte phaseInputId = nodeOperation.downstreamExecutionPhaseInputId();
        int bucketIdx = getBucketIdx(nodeOperation.executionPhase().nodeIds(), phaseInputId);

        MultiBucketBuilder multiBucketBuilder;
        switch (distributionInfo.distributionType()) {
            case MODULO:
                if (nodeOperation.downstreamNodes().size() == 1) {
                    multiBucketBuilder = new BroadcastingBucketBuilder(
                        streamers,
                        nodeOperation.downstreamNodes().size(),
                        ramAccounting
                    );
                } else {
                    multiBucketBuilder = new ModuloBucketBuilder(
                        streamers,
                        nodeOperation.downstreamNodes().size(),
                        distributionInfo.distributeByColumn(),
                        ramAccounting
                    );
                }
                break;
            case BROADCAST:
                multiBucketBuilder = new BroadcastingBucketBuilder(
                    streamers,
                    nodeOperation.downstreamNodes().size(),
                    ramAccounting
                );
                break;
            default:
                throw new UnsupportedOperationException("Can't handle distributionInfo: " + distributionInfo);
        }

        return new DistributingConsumer(
            responseExecutor,
            jobId,
            multiBucketBuilder,
            nodeOperation.downstreamExecutionPhaseId(),
            phaseInputId,
            bucketIdx,
            nodeOperation.downstreamNodes(),
            distributedResultAction,
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
     * See {@link PageBucketReceiver}
     */
    private int getBucketIdx(Collection<String> nodeIds, byte phaseInputId) {
        ArrayList<String> server = new ArrayList<>(nodeIds);
        server.sort(null);
        int nodeId = Math.max(server.indexOf(clusterService.localNode().getId()), 0);
        return nodeId | (phaseInputId << 24);
    }
}
