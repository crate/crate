/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.single.shard;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.SQLExceptions;

/**
 * A base class for operations that need to perform a read operation on a single shard copy. If the operation fails,
 * the read operation can be performed on other shard copies. Concrete implementations can provide their own list
 * of candidate shards to try the read operation on.
 */
public abstract class TransportSingleShardAction<Request extends SingleShardRequest, Response extends TransportResponse>
    extends TransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;

    private final String transportShardAction;
    private final String executor;

    protected final Logger logger = LogManager.getLogger(getClass());

    protected TransportSingleShardAction(String actionName,
                                         ThreadPool threadPool,
                                         ClusterService clusterService,
                                         TransportService transportService,
                                         Writeable.Reader<Request> request,
                                         String executor) {
        super(actionName);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;

        this.transportShardAction = actionName + "[s]";
        this.executor = executor;

        if (!isSubAction()) {
            transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, request, new TransportHandler());
        }
        transportService.registerRequestHandler(transportShardAction, ThreadPool.Names.SAME, request, new ShardTransportHandler());
    }

    /**
     * Tells whether the action is a main one or a subaction. Used to decide whether we need to register
     * the main transport handler. In fact if the action is a subaction, its execute method
     * will be called locally to its parent action.
     */
    protected boolean isSubAction() {
        return false;
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected abstract Response shardOperation(Request request, ShardId shardId) throws IOException;

    protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
        threadPool.executor(getExecutor(request, shardId))
            .execute(ActionRunnable.supply(listener, () -> shardOperation(request, shardId)));
    }

    protected abstract Writeable.Reader<Response> getResponseReader();

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    /**
     * Returns the candidate shards to execute the operation on or <code>null</code> the execute
     * the operation locally (the node that received the request)
     */
    @Nullable
    protected abstract ShardsIterator shards(ClusterState state, Request request);

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final ShardsIterator shardIt;
        private final Request request;
        private final DiscoveryNodes nodes;
        private volatile Exception lastFailure;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }


            ClusterBlocks blocks = clusterState.blocks();
            blockException = blocks.indexBlockedException(ClusterBlockLevel.READ, request.index());
            if (blockException != null) {
                throw blockException;
            }

            this.shardIt = shards(clusterState, request);
        }

        public void start() {
            if (shardIt == null) {
                // just execute it on the local node
                final Writeable.Reader<Response> reader = getResponseReader();
                transportService.sendRequest(
                    clusterService.localNode(),
                    transportShardAction,
                    request,
                    new ActionListenerResponseHandler<>(transportShardAction, listener, reader)
                );
            } else {
                perform(null);
            }
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> new ParameterizedMessage(
                    "{}: failed to execute [{}]", shardRouting, request), e);
            }
            perform(e);
        }

        private void perform(@Nullable final Exception currentFailure) {
            Exception lastFailure = this.lastFailure;
            if (lastFailure == null || !SQLExceptions.isShardNotAvailable(currentFailure)) {
                lastFailure = currentFailure;
                this.lastFailure = currentFailure;
            }
            final ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                Exception failure = lastFailure;
                if (failure == null || SQLExceptions.isShardNotAvailable(failure)) {
                    failure = new NoShardAvailableActionException(
                        null,
                        LoggerMessageFormat.format("No shard available for [{}]", request),
                        failure
                    );
                } else {
                    logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null, request), failure);
                }
                listener.onFailure(failure);
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                request.internalShardId = shardRouting.shardId();
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "sending request [{}] to shard [{}] on node [{}]",
                        request,
                        request.internalShardId,
                        node
                    );
                }
                final Writeable.Reader<Response> reader = getResponseReader();
                transportService.sendRequest(
                    node,
                    transportShardAction,
                    request,
                    new TransportResponseHandler<Response>() {

                        @Override
                        public Response read(StreamInput in) throws IOException {
                            return reader.read(in);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public void handleResponse(final Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(shardRouting, exp);
                        }
                    }
                );
            }
        }
    }

    private class TransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // if we have a local operation, execute it on a thread since we don't spawn
            execute(request).whenComplete(new ChannelActionListener<>(channel));
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] on shard [{}]", request, request.internalShardId);
            }
            asyncShardOperation(request, request.internalShardId, new ChannelActionListener<>(channel));
        }
    }

    protected String getExecutor(Request request, ShardId shardId) {
        return executor;
    }
}
