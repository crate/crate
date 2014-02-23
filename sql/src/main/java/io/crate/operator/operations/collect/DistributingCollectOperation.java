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

package io.crate.operator.operations.collect;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.projectors.Projector;
import io.crate.planner.node.CollectNode;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * handling distributing collect requests
 * collected data is distributed to downStream nodes that further merge/reduce their data
 */
public class DistributingCollectOperation extends MapSideDataCollectOperation {

    public static class DistributingShardCollectFuture extends ShardCollectFuture {

        private final ESLogger logger = Loggers.getLogger(getClass());

        private final TransportService transportService;
        private final DistributedResultRequest[] requests;
        private final List<DiscoveryNode> downStreams;
        private final int numDownStreams;
        private final UUID jobId;


        public DistributingShardCollectFuture(UUID jobId,
                                              int numShards,
                                              List<Projector> projectorChain,
                                              List<DiscoveryNode> downStreams,
                                              TransportService transportService,
                                              DataType.Streamer<?>[] streamers) {
            super(numShards, projectorChain);
            Preconditions.checkNotNull(downStreams);
            Preconditions.checkNotNull(jobId);
            this.jobId = jobId;
            this.transportService = transportService;
            this.downStreams = downStreams;
            this.numDownStreams = this.downStreams.size();

            this.requests = new DistributedResultRequest[numDownStreams];
            for (int i=0, length = this.downStreams.size(); i<length; i++) {
                this.requests[i] = new DistributedResultRequest(jobId, streamers);
            }
        }

        @Override
        protected void onAllShardsFinished() {
            Throwable throwable = lastException.get();
            if (throwable != null) {
                setException(throwable);
                forwardFailures();
                return;
            }
            super.set(Constants.EMPTY_RESULT);

            projectorChain.get(0).finishProjection();
            BucketingIterator bucketingIterator = new ModuloBucketingIterator(
                    this.numDownStreams,
                    projectorChain.get(projectorChain.size()-1)
            );

            // send requests
            int i = 0;
            for (List<Object[]> bucket : bucketingIterator) {
                DistributedResultRequest request = this.requests[i];
                request.rows(bucket.toArray(new Object[bucket.size()][]));
                final DiscoveryNode node = downStreams.get(i);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] sending distributing collect request to {} ...",
                            jobId.toString(),
                            node.id());
                }
                sendRequest(request, node);
                i++;
            }
        }

        private void forwardFailures() {
            int idx = 0;
            for (DistributedResultRequest request : requests) {
                request.failure(true);
                sendRequest(request, downStreams.get(idx));
                idx++;
            }
        }

        private void sendRequest(DistributedResultRequest request, final DiscoveryNode node) {
            transportService.submitRequest(
                node,
                TransportMergeNodeAction.mergeRowsAction, // NOTICE: hard coded transport action, should be delivered by collectNode
                request,
                new BaseTransportResponseHandler<DistributedResultResponse>() {
                    @Override
                    public DistributedResultResponse newInstance() {
                        return new DistributedResultResponse();
                    }

                    @Override
                    public void handleResponse(DistributedResultResponse response) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] successfully sent distributing collect request to {}",
                                    jobId.toString(),
                                    node.id());
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[{}] Exception sending distributing collect request to {}",
                                exp,
                                jobId.toString(),
                                node.id());
                        setException(exp.getCause());
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SEARCH;
                    }
                }
            );
        }
    }

    private final TransportService transportService;
    private final PlanNodeStreamerVisitor streamerVisitor;

    @Inject
    public DistributingCollectOperation(ClusterService clusterService,
                                        Functions functions,
                                        ReferenceResolver referenceResolver,
                                        IndicesService indicesService,
                                        ThreadPool threadPool,
                                        TransportService transportService,
                                        PlanNodeStreamerVisitor streamerVisitor) {
        super(clusterService, functions, referenceResolver, indicesService, threadPool);
        this.transportService = transportService;
        this.streamerVisitor = streamerVisitor;
    }

    @Override
    protected ListenableFuture<Object[][]> handleNodeCollect(CollectNode collectNode) {
        // the planner optimizes group by on node granularity so this is never used/called.
        throw new UnsupportedOperationException("distributing collect not needed for node level statements");
    }

    @Override
    protected ListenableFuture<Object[][]> handleShardCollect(CollectNode collectNode) {
        assert collectNode.hasDownstreams() : "no downstreams";
        return super.handleShardCollect(collectNode);
    }

    @Override
    protected ShardCollectFuture getShardCollectFuture(int numShards, List<Projector> projectors, CollectNode collectNode) {
        List<DiscoveryNode> downStreams = new ArrayList<>(collectNode.downStreamNodes().size());
        for (String nodeId : collectNode.downStreamNodes()) {
            DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            downStreams.add(node);
        }
        assert collectNode.jobId().isPresent();
        DataType.Streamer<?>[] streamers = streamerVisitor.process(collectNode).outputStreamers();
        return new DistributingShardCollectFuture(
                collectNode.jobId().get(),
                numShards,
                projectors,
                downStreams,
                transportService,
                streamers
                );
    }
}
