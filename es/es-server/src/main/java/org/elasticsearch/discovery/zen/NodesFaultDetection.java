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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * A fault detection of multiple nodes.
 */
public class NodesFaultDetection extends FaultDetection {

    public static final String PING_ACTION_NAME = "internal:discovery/zen/fd/ping";

    public abstract static class Listener {

        public void onNodeFailure(DiscoveryNode node, String reason) {}

        public void onPingReceived(PingRequest pingRequest) {}

    }

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private final ConcurrentMap<DiscoveryNode, NodeFD> nodesFD = newConcurrentMap();

    private final Supplier<ClusterState> clusterStateSupplier;

    private volatile DiscoveryNode localNode;

    public NodesFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService,
                               Supplier<ClusterState> clusterStateSupplier, ClusterName clusterName) {
        super(settings, threadPool, transportService, clusterName);

        this.clusterStateSupplier = clusterStateSupplier;

        logger.debug("[node  ] uses ping_interval [{}], ping_timeout [{}], ping_retries [{}]", pingInterval, pingRetryTimeout,
            pingRetryCount);

        transportService.registerRequestHandler(
            PING_ACTION_NAME, PingRequest::new, ThreadPool.Names.SAME, false, false, new PingRequestHandler());
    }

    public void setLocalNode(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    /**
     * Gets the current set of nodes involved in node fault detection.
     * NB: For testing purposes.
     */
    public Set<DiscoveryNode> getNodes() {
        return Collections.unmodifiableSet(nodesFD.keySet());
    }

    /**
     * make sure that nodes in clusterState are pinged. Any pinging to nodes which are not
     * part of the cluster will be stopped
     */
    public void updateNodesAndPing(ClusterState clusterState) {
        // remove any nodes we don't need, this will cause their FD to stop
        for (DiscoveryNode monitoredNode : nodesFD.keySet()) {
            if (!clusterState.nodes().nodeExists(monitoredNode)) {
                nodesFD.remove(monitoredNode);
            }
        }
        // add any missing nodes

        for (DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                // no need to monitor the local node
                continue;
            }
            if (!nodesFD.containsKey(node)) {
                NodeFD fd = new NodeFD(node);
                // it's OK to overwrite an existing nodeFD - it will just stop and the new one will pick things up.
                nodesFD.put(node, fd);
                // we use schedule with a 0 time value to run the pinger on the pool as it will run on later
                threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, fd);
            }
        }
    }

    /** stops all pinging **/
    public NodesFaultDetection stop() {
        nodesFD.clear();
        return this;
    }

    @Override
    public void close() {
        super.close();
        stop();
    }

    @Override
    protected void handleTransportDisconnect(DiscoveryNode node) {
        NodeFD nodeFD = nodesFD.remove(node);
        if (nodeFD == null) {
            return;
        }
        if (connectOnNetworkDisconnect) {
            NodeFD fd = new NodeFD(node);
            try {
                transportService.connectToNode(node);
                nodesFD.put(node, fd);
                // we use schedule with a 0 time value to run the pinger on the pool as it will run on later
                threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, fd);
            } catch (Exception e) {
                logger.trace("[node  ] [{}] transport disconnected (with verified connect)", node);
                // clean up if needed, just to be safe..
                nodesFD.remove(node, fd);
                notifyNodeFailure(node, "transport disconnected (with verified connect)");
            }
        } else {
            logger.trace("[node  ] [{}] transport disconnected", node);
            notifyNodeFailure(node, "transport disconnected");
        }
    }

    private void notifyNodeFailure(final DiscoveryNode node, final String reason) {
        try {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    for (Listener listener : listeners) {
                        listener.onNodeFailure(node, reason);
                    }
                }
            });
        } catch (EsRejectedExecutionException ex) {
            logger.trace(() -> new ParameterizedMessage(
                    "[node  ] [{}] ignoring node failure (reason [{}]). Local node is shutting down", node, reason), ex);
        }
    }

    private void notifyPingReceived(final PingRequest pingRequest) {
        threadPool.generic().execute(new Runnable() {

            @Override
            public void run() {
                for (Listener listener : listeners) {
                    listener.onPingReceived(pingRequest);
                }
            }

        });
    }


    private class NodeFD implements Runnable {
        volatile int retryCount;

        private final DiscoveryNode node;

        private NodeFD(DiscoveryNode node) {
            this.node = node;
        }

        private boolean running() {
            return NodeFD.this.equals(nodesFD.get(node));
        }

        private PingRequest newPingRequest() {
            return new PingRequest(node, clusterName, localNode, clusterStateSupplier.get().version());
        }

        @Override
        public void run() {
            if (!running()) {
                return;
            }
            final TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.PING)
                .withTimeout(pingRetryTimeout).build();
            transportService.sendRequest(node, PING_ACTION_NAME, newPingRequest(), options, new TransportResponseHandler<PingResponse>() {
                        @Override
                        public PingResponse newInstance() {
                            return new PingResponse();
                        }

                        @Override
                        public void handleResponse(PingResponse response) {
                            if (!running()) {
                                return;
                            }
                            retryCount = 0;
                            threadPool.schedule(pingInterval, ThreadPool.Names.SAME, NodeFD.this);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            if (!running()) {
                                return;
                            }
                            if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                                handleTransportDisconnect(node);
                                return;
                            }

                            retryCount++;
                            logger.trace( () -> new ParameterizedMessage(
                                    "[node  ] failed to ping [{}], retry [{}] out of [{}]", node, retryCount, pingRetryCount), exp);
                            if (retryCount >= pingRetryCount) {
                                logger.debug("[node  ] failed to ping [{}], tried [{}] times, each with  maximum [{}] timeout", node,
                                    pingRetryCount, pingRetryTimeout);
                                // not good, failure
                                if (nodesFD.remove(node, NodeFD.this)) {
                                    notifyNodeFailure(node, "failed to ping, tried [" + pingRetryCount + "] times, each with maximum ["
                                        + pingRetryTimeout + "] timeout");
                                }
                            } else {
                                // resend the request, not reschedule, rely on send timeout
                                transportService.sendRequest(node, PING_ACTION_NAME, newPingRequest(), options, this);
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

    class PingRequestHandler implements TransportRequestHandler<PingRequest> {
        @Override
        public void messageReceived(PingRequest request, TransportChannel channel) throws Exception {
            // if we are not the node we are supposed to be pinged, send an exception
            // this can happen when a kill -9 is sent, and another node is started using the same port
            if (!localNode.equals(request.targetNode())) {
                throw new IllegalStateException("Got pinged as node " + request.targetNode() + "], but I am node " + localNode );
            }

            // PingRequest will have clusterName set to null if it came from a node of version <1.4.0
            if (request.clusterName != null && !request.clusterName.equals(clusterName)) {
                // Don't introduce new exception for bwc reasons
                throw new IllegalStateException("Got pinged with cluster name [" + request.clusterName + "], but I'm part of cluster ["
                    + clusterName + "]");
            }

            notifyPingReceived(request);

            channel.sendResponse(new PingResponse());
        }
    }


    public static class PingRequest extends TransportRequest {

        // the (assumed) node we are pinging
        private DiscoveryNode targetNode;

        private ClusterName clusterName;

        private DiscoveryNode masterNode;

        private long clusterStateVersion = ClusterState.UNKNOWN_VERSION;

        public PingRequest() {
        }

        PingRequest(DiscoveryNode targetNode, ClusterName clusterName, DiscoveryNode masterNode, long clusterStateVersion) {
            this.targetNode = targetNode;
            this.clusterName = clusterName;
            this.masterNode = masterNode;
            this.clusterStateVersion = clusterStateVersion;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
        }

        public ClusterName clusterName() {
            return clusterName;
        }

        public DiscoveryNode masterNode() {
            return masterNode;
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            targetNode = new DiscoveryNode(in);
            clusterName = new ClusterName(in);
            masterNode = new DiscoveryNode(in);
            clusterStateVersion = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            targetNode.writeTo(out);
            clusterName.writeTo(out);
            masterNode.writeTo(out);
            out.writeLong(clusterStateVersion);
        }
    }

    private static class PingResponse extends TransportResponse {

        private PingResponse() {
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
