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

package org.elasticsearch.action.support.master;

import java.io.IOException;
import java.util.function.Predicate;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;

/**
 * A base class for operations that needs to be performed on the master node.
 */
public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends TransportResponse>
    extends HandledTransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;

    private final String executor;

    protected TransportMasterNodeAction(String actionName,
                                        TransportService transportService,
                                        ClusterService clusterService,
                                        ThreadPool threadPool,
                                        Writeable.Reader<Request> request) {
        this(actionName, true, transportService, clusterService, threadPool, request);
    }

    protected TransportMasterNodeAction(String actionName,
                                        boolean canTripCircuitBreaker,
                                        TransportService transportService,
                                        ClusterService clusterService,
                                        ThreadPool threadPool,
                                        Writeable.Reader<Request> request) {
        super(actionName, canTripCircuitBreaker, transportService, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response read(StreamInput in) throws IOException;

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception;

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected void doExecute(final Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    class WaitForInitialState implements ClusterStateListener {

        private final TransportMasterNodeAction<Request, Response>.AsyncSingleAction asyncSingleAction;

        public WaitForInitialState(TransportMasterNodeAction<Request, Response>.AsyncSingleAction asyncSingleAction) {
            this.asyncSingleAction = asyncSingleAction;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            clusterService.removeListener(this);
            asyncSingleAction.start();
        }
    }

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterStateObserver observer;

        AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            ClusterState state = clusterService.state();
            if (state == null) {
                WaitForInitialState waitForState = new WaitForInitialState(this);
                clusterService.addListener(waitForState);
                // protect against race between state null check and listener registration
                if (clusterService.state() != null) {
                    waitForState.clusterChanged(null);
                }
                return;
            }
            this.observer = new ClusterStateObserver(state, clusterService, request.masterNodeTimeout(), logger);
            doStart(state);
        }

        protected void doStart(ClusterState clusterState) {
            try {
                final Predicate<ClusterState> masterChangePredicate = MasterNodeChangePredicate.build(clusterState);
                final DiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    final ClusterBlockException blockException = checkBlock(request, clusterState);
                    if (blockException != null) {
                        if (!blockException.retryable()) {
                            listener.onFailure(blockException);
                        } else {
                            logger.trace("can't execute due to a cluster block, retrying", blockException);
                            retry(blockException, newState -> {
                                try {
                                    ClusterBlockException newException = checkBlock(request, newState);
                                    return (newException == null || !newException.retryable());
                                } catch (Exception e) {
                                    // accept state as block will be rechecked by doStart() and listener.onFailure() then called
                                    logger.trace("exception occurred during cluster block checking, accepting state", e);
                                    return true;
                                }
                            });
                        }
                    } else {
                        ActionListener<Response> delegate = ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
                            if (t instanceof FailedToCommitClusterStateException || t instanceof NotMasterException) {
                                logger.debug(() -> new ParameterizedMessage("master could not publish cluster state or " +
                                    "stepped down before publishing action [{}], scheduling a retry", actionName), t);
                                retry(t, masterChangePredicate);
                            } else {
                                delegatedListener.onFailure(t);
                            }
                        });
                        threadPool.executor(executor)
                            .execute(ActionRunnable.wrap(delegate, l -> masterOperation(request, clusterState, l)));
                    }
                } else {
                    if (nodes.getMasterNode() == null) {
                        logger.debug("no known master node, scheduling a retry");
                        retry(null, masterChangePredicate);
                    } else {
                        DiscoveryNode masterNode = nodes.getMasterNode();
                        final String actionName = getMasterActionName(masterNode);
                        transportService.sendRequest(masterNode, actionName, request, new ActionListenerResponseHandler<Response>(listener,
                            TransportMasterNodeAction.this::read) {
                            @Override
                            public void handleException(final TransportException exp) {
                                Throwable cause = exp.unwrapCause();
                                if (cause instanceof ConnectTransportException ||
                                        (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                    // we want to retry here a bit to see if a new master is elected
                                    logger.debug("connection exception while trying to forward request with action name [{}] to " +
                                            "master node [{}], scheduling a retry. Error: [{}]",
                                        actionName, nodes.getMasterNode(), exp.getDetailedMessage());
                                    retry(cause, masterChangePredicate);
                                } else {
                                    listener.onFailure(exp);
                                }
                            }
                        });
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retry(final Throwable failure, final Predicate<ClusterState> statePredicate) {
            observer.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        doStart(state);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.debug(() -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])",
                            actionName, timeout), failure);
                        listener.onFailure(new MasterNotDiscoveredException(failure));
                    }
                }, statePredicate
            );
        }
    }

    /**
     * Allows to conditionally return a different master node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     */
    protected String getMasterActionName(DiscoveryNode node) {
        return actionName;
    }
}
