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

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A fault detection that pings the master periodically to see if its alive.
 */
public class MasterFaultDetection extends FaultDetection {

    public static final String MASTER_PING_ACTION_NAME = "internal:discovery/zen/fd/master_ping";

    public interface Listener {

        /** called when pinging the master failed, like a timeout, transport disconnects etc */
        void onMasterFailure(DiscoveryNode masterNode, Throwable cause, String reason);

    }

    private final MasterService masterService;
    private final java.util.function.Supplier<ClusterState> clusterStateSupplier;
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private volatile MasterPinger masterPinger;

    private final Object masterNodeMutex = new Object();

    private volatile DiscoveryNode masterNode;

    private volatile int retryCount;

    private final AtomicBoolean notifiedMasterFailure = new AtomicBoolean();

    public MasterFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService,
                                java.util.function.Supplier<ClusterState> clusterStateSupplier, MasterService masterService,
                                ClusterName clusterName) {
        super(settings, threadPool, transportService, clusterName);
        this.clusterStateSupplier = clusterStateSupplier;
        this.masterService = masterService;

        logger.debug("[master] uses ping_interval [{}], ping_timeout [{}], ping_retries [{}]", pingInterval, pingRetryTimeout,
            pingRetryCount);

        transportService.registerRequestHandler(
            MASTER_PING_ACTION_NAME, MasterPingRequest::new, ThreadPool.Names.SAME, false, false, new MasterPingRequestHandler());
    }

    public DiscoveryNode masterNode() {
        return this.masterNode;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void restart(DiscoveryNode masterNode, String reason) {
        synchronized (masterNodeMutex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] restarting fault detection against master [{}], reason [{}]", masterNode, reason);
            }
            innerStop();
            innerStart(masterNode);
        }
    }

    private void innerStart(final DiscoveryNode masterNode) {
        this.masterNode = masterNode;
        this.retryCount = 0;
        this.notifiedMasterFailure.set(false);
        if (masterPinger != null) {
            masterPinger.stop();
        }
        this.masterPinger = new MasterPinger();

        // we start pinging slightly later to allow the chosen master to complete it's own master election
        threadPool.schedule(pingInterval, ThreadPool.Names.SAME, masterPinger);
    }

    public void stop(String reason) {
        synchronized (masterNodeMutex) {
            if (masterNode != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[master] stopping fault detection against master [{}], reason [{}]", masterNode, reason);
                }
            }
            innerStop();
        }
    }

    private void innerStop() {
        // also will stop the next ping schedule
        this.retryCount = 0;
        if (masterPinger != null) {
            masterPinger.stop();
            masterPinger = null;
        }
        this.masterNode = null;
    }

    @Override
    public void close() {
        super.close();
        stop("closing");
        this.listeners.clear();
    }

    @Override
    protected void handleTransportDisconnect(DiscoveryNode node) {
        synchronized (masterNodeMutex) {
            if (!node.equals(this.masterNode)) {
                return;
            }
            if (connectOnNetworkDisconnect) {
                try {
                    transportService.connectToNode(node);
                    // if all is well, make sure we restart the pinger
                    if (masterPinger != null) {
                        masterPinger.stop();
                    }
                    this.masterPinger = new MasterPinger();
                    // we use schedule with a 0 time value to run the pinger on the pool as it will run on later
                    threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, masterPinger);
                } catch (Exception e) {
                    logger.trace("[master] [{}] transport disconnected (with verified connect)", masterNode);
                    notifyMasterFailure(masterNode, null, "transport disconnected (with verified connect)");
                }
            } else {
                logger.trace("[master] [{}] transport disconnected", node);
                notifyMasterFailure(node, null, "transport disconnected");
            }
        }
    }

    private void notifyMasterFailure(final DiscoveryNode masterNode, final Throwable cause, final String reason) {
        if (notifiedMasterFailure.compareAndSet(false, true)) {
            try {
                threadPool.generic().execute(() -> {
                    for (Listener listener : listeners) {
                        listener.onMasterFailure(masterNode, cause, reason);
                    }
                });
            } catch (EsRejectedExecutionException e) {
                logger.error("master failure notification was rejected, it's highly likely the node is shutting down", e);
            }
            stop("master failure, " + reason);
        }
    }

    private class MasterPinger implements Runnable {

        private volatile boolean running = true;

        public void stop() {
            this.running = false;
        }

        @Override
        public void run() {
            if (!running) {
                // return and don't spawn...
                return;
            }
            final DiscoveryNode masterToPing = masterNode;
            if (masterToPing == null) {
                // master is null, should not happen, but we are still running, so reschedule
                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this);
                return;
            }

            final MasterPingRequest request = new MasterPingRequest(
                clusterStateSupplier.get().nodes().getLocalNode(), masterToPing, clusterName);
            final TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.PING)
                .withTimeout(pingRetryTimeout).build();
            transportService.sendRequest(masterToPing, MASTER_PING_ACTION_NAME, request, options,
                new TransportResponseHandler<MasterPingResponseResponse>() {
                        @Override
                        public MasterPingResponseResponse newInstance() {
                            return new MasterPingResponseResponse();
                        }

                        @Override
                        public void handleResponse(MasterPingResponseResponse response) {
                            if (!running) {
                                return;
                            }
                            // reset the counter, we got a good result
                            MasterFaultDetection.this.retryCount = 0;
                            // check if the master node did not get switched on us..., if it did, we simply return with no reschedule
                            if (masterToPing.equals(MasterFaultDetection.this.masterNode())) {
                                // we don't stop on disconnection from master, we keep pinging it
                                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (!running) {
                                return;
                            }
                            synchronized (masterNodeMutex) {
                                // check if the master node did not get switched on us...
                                if (masterToPing.equals(MasterFaultDetection.this.masterNode())) {
                                    if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                                        handleTransportDisconnect(masterToPing);
                                        return;
                                    } else if (exp.getCause() instanceof NotMasterException) {
                                        logger.debug("[master] pinging a master {} that is no longer a master", masterNode);
                                        notifyMasterFailure(masterToPing, exp, "no longer master");
                                        return;
                                    } else if (exp.getCause() instanceof ThisIsNotTheMasterYouAreLookingForException) {
                                        logger.debug("[master] pinging a master {} that is not the master", masterNode);
                                        notifyMasterFailure(masterToPing, exp,"not master");
                                        return;
                                    } else if (exp.getCause() instanceof NodeDoesNotExistOnMasterException) {
                                        logger.debug("[master] pinging a master {} but we do not exists on it, act as if its master failure"
                                            , masterNode);
                                        notifyMasterFailure(masterToPing, exp,"do not exists on master, act as master failure");
                                        return;
                                    }

                                    int retryCount = ++MasterFaultDetection.this.retryCount;
                                    logger.trace(() -> new ParameterizedMessage(
                                            "[master] failed to ping [{}], retry [{}] out of [{}]",
                                            masterNode, retryCount, pingRetryCount), exp);
                                    if (retryCount >= pingRetryCount) {
                                        logger.debug("[master] failed to ping [{}], tried [{}] times, each with maximum [{}] timeout",
                                            masterNode, pingRetryCount, pingRetryTimeout);
                                        // not good, failure
                                        notifyMasterFailure(masterToPing, null, "failed to ping, tried [" + pingRetryCount
                                            + "] times, each with  maximum [" + pingRetryTimeout + "] timeout");
                                    } else {
                                        // resend the request, not reschedule, rely on send timeout
                                        transportService.sendRequest(masterToPing, MASTER_PING_ACTION_NAME, request, options, this);
                                    }
                                }
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
            );
        }
    }

    /** Thrown when a ping reaches the wrong node */
    static class ThisIsNotTheMasterYouAreLookingForException extends IllegalStateException {

        ThisIsNotTheMasterYouAreLookingForException(String msg) {
            super(msg);
        }

        ThisIsNotTheMasterYouAreLookingForException() {
        }

        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    static class NodeDoesNotExistOnMasterException extends IllegalStateException {
        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    private class MasterPingRequestHandler implements TransportRequestHandler<MasterPingRequest> {

        @Override
        public void messageReceived(final MasterPingRequest request, final TransportChannel channel) throws Exception {
            final DiscoveryNodes nodes = clusterStateSupplier.get().nodes();
            // check if we are really the same master as the one we seemed to be think we are
            // this can happen if the master got "kill -9" and then another node started using the same port
            if (!request.masterNode.equals(nodes.getLocalNode())) {
                throw new ThisIsNotTheMasterYouAreLookingForException();
            }

            // ping from nodes of version < 1.4.0 will have the clustername set to null
            if (request.clusterName != null && !request.clusterName.equals(clusterName)) {
                logger.trace("master fault detection ping request is targeted for a different [{}] cluster then us [{}]",
                    request.clusterName, clusterName);
                throw new ThisIsNotTheMasterYouAreLookingForException("master fault detection ping request is targeted for a different ["
                    + request.clusterName + "] cluster then us [" + clusterName + "]");
            }

            // when we are elected as master or when a node joins, we use a cluster state update thread
            // to incorporate that information in the cluster state. That cluster state is published
            // before we make it available locally. This means that a master ping can come from a node
            // that has already processed the new CS but it is not known locally.
            // Therefore, if we fail we have to check again under a cluster state thread to make sure
            // all processing is finished.
            //

            if (!nodes.isLocalNodeElectedMaster() || !nodes.nodeExists(request.sourceNode)) {
                logger.trace("checking ping from {} under a cluster state thread", request.sourceNode);
                masterService.submitStateUpdateTask("master ping (from: " + request.sourceNode + ")", new ClusterStateUpdateTask() {

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        // if we are no longer master, fail...
                        DiscoveryNodes nodes = currentState.nodes();
                        if (!nodes.nodeExists(request.sourceNode)) {
                            throw new NodeDoesNotExistOnMasterException();
                        }
                        return currentState;
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        onFailure(source, new NotMasterException("local node is not master"));
                    }

                    @Override
                    public void onFailure(String source, @Nullable Exception e) {
                        if (e == null) {
                            e = new ElasticsearchException("unknown error while processing ping");
                        }
                        try {
                            channel.sendResponse(e);
                        } catch (IOException inner) {
                            inner.addSuppressed(e);
                            logger.warn("error while sending ping response", inner);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(new MasterPingResponseResponse());
                        } catch (IOException e) {
                            logger.warn("error while sending ping response", e);
                        }
                    }
                });
            } else {
                // send a response, and note if we are connected to the master or not
                channel.sendResponse(new MasterPingResponseResponse());
            }
        }
    }


    public static class MasterPingRequest extends TransportRequest {

        private DiscoveryNode sourceNode;

        private DiscoveryNode masterNode;
        private ClusterName clusterName;

        public MasterPingRequest() {
        }

        private MasterPingRequest(DiscoveryNode sourceNode, DiscoveryNode masterNode, ClusterName clusterName) {
            this.sourceNode = sourceNode;
            this.masterNode = masterNode;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            sourceNode = new DiscoveryNode(in);
            masterNode = new DiscoveryNode(in);
            clusterName = new ClusterName(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            masterNode.writeTo(out);
            clusterName.writeTo(out);
        }
    }

    private static class MasterPingResponseResponse extends TransportResponse {

        private MasterPingResponseResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
