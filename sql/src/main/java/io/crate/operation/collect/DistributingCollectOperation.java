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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.Streamer;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.executor.transport.distributed.DistributedFailureRequest;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.executor.transport.distributed.MultiBucketBuilder;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * handling distributing collect requests
 * collected data is distributed to downstream nodes that further merge/reduce their data
 */
@Singleton
public class DistributingCollectOperation extends MapSideDataCollectOperation<MultiBucketBuilder> {

    private final static ESLogger logger = Loggers.getLogger(DistributingCollectOperation.class);

    public static class DistributingShardCollectFuture extends ShardCollectFuture {

        private static final ESLogger logger = Loggers.getLogger(DistributingCollectOperation.class);

        private final TransportService transportService;
        private final DistributedResultRequest[] requests;
        private final List<DiscoveryNode> downStreams;
        private final int numDownStreams;
        private final UUID jobId;
        private final MultiBucketBuilder bucketBuilder;

        public DistributingShardCollectFuture(UUID jobId,
                                              int numShards,
                                              MultiBucketBuilder bucketBuilder,
                                              List<DiscoveryNode> downStreams,
                                              TransportService transportService,
                                              Streamer<?>[] streamers) {
            super(numShards);
            this.bucketBuilder = bucketBuilder;
            Preconditions.checkNotNull(downStreams, "downstream nodes is null");
            Preconditions.checkNotNull(jobId, "jobId is null");
            this.jobId = jobId;
            this.transportService = transportService;
            this.downStreams = downStreams;
            this.numDownStreams = this.downStreams.size();

            this.requests = new DistributedResultRequest[numDownStreams];
            for (int i = 0, length = this.downStreams.size(); i < length; i++) {
                this.requests[i] = new DistributedResultRequest(jobId, streamers);
            }
        }

        @Override
        public void onAllShardsFinished() {
            Throwable throwable = lastException.get();
            if (throwable != null) {
                setException(throwable);
                forwardFailures();
                return;
            }
            int i = 0;
            for (Bucket rows : bucketBuilder.build()) {
                DistributedResultRequest request = this.requests[i];
                request.rows(rows);
                final DiscoveryNode node = downStreams.get(i);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] sending distributing collect request to {} ...",
                            jobId.toString(),
                            node.id());
                }
                sendRequest(request, node);
                i++;
            }
            super.set(Bucket.EMPTY);
        }

        private void forwardFailures() {
            int idx = 0;
            for (DistributedResultRequest request : requests) {
                request.failure(true);
                sendRequest(request, downStreams.get(idx));
                idx++;
            }
        }

        private void sendRequest(final DistributedResultRequest request, final DiscoveryNode node) {
            transportService.submitRequest(
                    node,
                    TransportMergeNodeAction.mergeRowsAction,
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
                            Throwable cause = Exceptions.unwrap(exp);
                            if (cause instanceof EsRejectedExecutionException) {
                                sendFailure(request.contextId(), node);
                            } else {
                                logger.error("[{}] Exception sending distributing collect request to {}",
                                        exp, jobId, node.id());
                                setException(cause);
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
            );
        }

        private void sendFailure(UUID contextId, final DiscoveryNode node) {
            transportService.submitRequest(
                    node,
                    TransportMergeNodeAction.failAction,
                    new DistributedFailureRequest(contextId),
                    new BaseTransportResponseHandler<DistributedResultResponse>() {
                        @Override
                        public DistributedResultResponse newInstance() {
                            return new DistributedResultResponse();
                        }

                        @Override
                        public void handleResponse(DistributedResultResponse response) {
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.error("[{}] Exception sending distributing collect failure to {}",
                                    exp, jobId, node.id());
                            setException(exp.getCause());
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
            );
        }
    }

    private List<DistributedResultRequest> genRequests(CollectNode node) {



        List<DistributedResultRequest> requests = new ArrayList<>(node.downStreamNodes().size());
        Streamer<?>[] streamers = getStreamers(node);
        for (int i = 0; i < node.downStreamNodes().size(); i++) {
            requests.add(new DistributedResultRequest(node.jobId().get(), streamers));
        }
        return requests;
    }

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
                                        PlanNodeStreamerVisitor streamerVisitor,
                                        CollectServiceResolver collectServiceResolver,
                                        CollectContextService collectContextService,
                                        CrateCircuitBreakerService breakerService) {
        super(clusterService, settings, transportActionProvider,
                functions, referenceResolver, indicesService,
                threadPool, collectServiceResolver, streamerVisitor, collectContextService);
        this.transportService = transportService;
        this.circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.QUERY_BREAKER);
    }

    @Override
    protected MultiBucketBuilder handleNodeCollect(CollectNode collectNode, RamAccountingContext ramAccountingContext) throws Exception {
        assert collectNode.jobId().isPresent();
        assert collectNode.hasDownstreams() : "distributing collect without downStreams";
        MultiBucketBuilder bucketBuilder = super.handleNodeCollect(collectNode, ramAccountingContext);
        sendRequestsOnFinish(collectNode, bucketBuilder);
        return bucketBuilder;
    }

    private void sendRequests(CollectNode collectNode,
                              MultiBucketBuilder bucketBuilder) {
        final List<DiscoveryNode> downStreams = toDiscoveryNodes(collectNode.downStreamNodes());
        final List<DistributedResultRequest> requests = genRequests(collectNode);
        int i = 0;
        for (Bucket rows : bucketBuilder.build()) {
            DistributedResultRequest request = requests.get(i);
            request.rows(rows);
            final DiscoveryNode node = downStreams.get(i);
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] sending distributing collect request to {} ...",
                        collectNode.jobId().get().toString(),
                        node.id());
            }
            sendRequest(request, node);
            i++;
        }
    }

    private void sendFailures(CollectNode collectNode) {
        final List<DiscoveryNode> downStreams = toDiscoveryNodes(collectNode.downStreamNodes());
        int idx = 0;
        for (DistributedResultRequest request : genRequests(collectNode)) {
            request.failure(true);
            sendRequest(request, downStreams.get(idx));
            idx++;
        }
    }

    private void sendRequestsOnFinish(
            final CollectNode collectNode,
            final MultiBucketBuilder bucketBuilder) {
        Futures.addCallback(bucketBuilder.result(), new FutureCallback<Bucket>() {
            @Override
            public void onSuccess(Bucket result) {
                assert result != null;
                sendRequests(collectNode, bucketBuilder);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                sendFailures(collectNode);
            }
        });
    }

    private void sendRequest(DistributedResultRequest request, DiscoveryNode discoveryNode) {
        transportService.sendRequest(
                discoveryNode,
                TransportMergeNodeAction.mergeRowsAction,
                request,
                new BaseTransportResponseHandler<DistributedResultResponse>() {
                    @Override
                    public DistributedResultResponse newInstance() {
                        return new DistributedResultResponse();
                    }

                    @Override
                    public void handleResponse(DistributedResultResponse response) {
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error(exp.getMessage(), exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
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
    protected Optional<MultiBucketBuilder> createResultResultProvider(CollectNode node) {
        return Optional.of(new MultiBucketBuilder(getStreamers(node), node.downStreamNodes().size()));
    }

    @Override
    protected ShardCollectFuture getShardCollectFuture(int numShards, ShardProjectorChain projectorChain, CollectNode collectNode) {
        assert collectNode.jobId().isPresent();
        PlanNodeStreamerVisitor.Context streamerContext = new PlanNodeStreamerVisitor.Context(null);
        MultiBucketBuilder bucketBuilder = (MultiBucketBuilder) projectorChain.resultProvider();
        streamerVisitor.process(collectNode, streamerContext);
        Streamer<?>[] streamers = streamerVisitor.process(
                collectNode, new RamAccountingContext("dummy", circuitBreaker)).outputStreamers();
        return new DistributingShardCollectFuture(
                collectNode.jobId().get(),
                numShards,
                bucketBuilder,
                toDiscoveryNodes(collectNode.downStreamNodes()),
                transportService,
                streamers
        );
    }
}
