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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class UnicastZenPing extends AbstractComponent implements ZenPing {

    public static final String ACTION_NAME = "internal:discovery/zen/unicast";
    public static final Setting<Integer> DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING =
        Setting.intSetting("discovery.zen.ping.unicast.concurrent_connects", 10, 0, Property.NodeScope);
    public static final Setting<TimeValue> DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT =
        Setting.positiveTimeSetting("discovery.zen.ping.unicast.hosts.resolve_timeout", TimeValue.timeValueSeconds(5), Property.NodeScope);

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterName clusterName;

    private final PingContextProvider contextProvider;

    private final AtomicInteger pingingRoundIdGenerator = new AtomicInteger();

    private final Map<Integer, PingingRound> activePingingRounds = newConcurrentMap();

    // a list of temporal responses a node will return for a request (holds responses from other nodes)
    private final Queue<PingResponse> temporalResponses = ConcurrentCollections.newQueue();

    private final UnicastHostsProvider hostsProvider;

    protected final EsThreadPoolExecutor unicastZenPingExecutorService;

    private final TimeValue resolveTimeout;

    private volatile boolean closed = false;

    public UnicastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                          UnicastHostsProvider unicastHostsProvider, PingContextProvider contextProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.hostsProvider = unicastHostsProvider;
        this.contextProvider = contextProvider;

        final int concurrentConnects = DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.get(settings);

        resolveTimeout = DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.get(settings);
        logger.debug(
            "using concurrent_connects [{}], resolve_timeout [{}]",
            concurrentConnects,
            resolveTimeout);

        transportService.registerRequestHandler(ACTION_NAME, ThreadPool.Names.SAME, UnicastPingRequest::new,
            new UnicastPingRequestHandler());

        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_connect]");
        unicastZenPingExecutorService = EsExecutors.newScaling(
                nodeName() + "/" + "unicast_connect",
                0,
                concurrentConnects,
                60,
                TimeUnit.SECONDS,
                threadFactory,
                threadPool.getThreadContext());
    }

    /**
     * Resolves a list of hosts to a list of transport addresses. Each host is resolved into a transport address (or a collection of
     * addresses if the number of ports is greater than one). Host lookups are done in parallel using specified executor service up
     * to the specified resolve timeout.
     *
     * @param executorService  the executor service used to parallelize hostname lookups
     * @param logger           logger used for logging messages regarding hostname lookups
     * @param hosts            the hosts to resolve
     * @param limitPortCounts  the number of ports to resolve (should be 1 for non-local transport)
     * @param transportService the transport service
     * @param resolveTimeout   the timeout before returning from hostname lookups
     * @return a list of resolved transport addresses
     */
    public static List<TransportAddress> resolveHostsLists(
        final ExecutorService executorService,
        final Logger logger,
        final List<String> hosts,
        final int limitPortCounts,
        final TransportService transportService,
        final TimeValue resolveTimeout) {
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(transportService);
        Objects.requireNonNull(resolveTimeout);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        final List<Callable<TransportAddress[]>> callables =
            hosts
                .stream()
                .map(hn -> (Callable<TransportAddress[]>) () -> transportService.addressesFromString(hn, limitPortCounts))
                .collect(Collectors.toList());
        final List<Future<TransportAddress[]>> futures;
        try {
            futures = executorService.invokeAll(callables, resolveTimeout.nanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }
        final List<TransportAddress> transportAddresses = new ArrayList<>();
        final Set<TransportAddress> localAddresses = new HashSet<>();
        localAddresses.add(transportService.boundAddress().publishAddress());
        localAddresses.addAll(Arrays.asList(transportService.boundAddress().boundAddresses()));
        // ExecutorService#invokeAll guarantees that the futures are returned in the iteration order of the tasks so we can associate the
        // hostname with the corresponding task by iterating together
        final Iterator<String> it = hosts.iterator();
        for (final Future<TransportAddress[]> future : futures) {
            final String hostname = it.next();
            if (!future.isCancelled()) {
                assert future.isDone();
                try {
                    final TransportAddress[] addresses = future.get();
                    logger.trace("resolved host [{}] to {}", hostname, addresses);
                    for (int addressId = 0; addressId < addresses.length; addressId++) {
                        final TransportAddress address = addresses[addressId];
                        // no point in pinging ourselves
                        if (localAddresses.contains(address) == false) {
                            transportAddresses.add(address);
                        }
                    }
                } catch (final ExecutionException e) {
                    assert e.getCause() != null;
                    final String message = "failed to resolve host [" + hostname + "]";
                    logger.warn(message, e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // ignore
                }
            } else {
                logger.warn("timed out after [{}] resolving host [{}]", resolveTimeout, hostname);
            }
        }
        return Collections.unmodifiableList(transportAddresses);
    }

    private UnicastHostsProvider.HostsResolver createHostsResolver() {
        return (hosts, limitPortCounts) -> resolveHostsLists(unicastZenPingExecutorService, logger, hosts,
            limitPortCounts, transportService, resolveTimeout);
    }

    @Override
    public void close() {
        ThreadPool.terminate(unicastZenPingExecutorService, 10, TimeUnit.SECONDS);
        Releasables.close(activePingingRounds.values());
        closed = true;
    }

    @Override
    public void start() {
    }

    /**
     * Clears the list of cached ping responses.
     */
    public void clearTemporalResponses() {
        temporalResponses.clear();
    }

    /**
     * Sends three rounds of pings notifying the specified {@link Consumer} when pinging is complete. Pings are sent after resolving
     * configured unicast hosts to their IP address (subject to DNS caching within the JVM). A batch of pings is sent, then another batch
     * of pings is sent at half the specified {@link TimeValue}, and then another batch of pings is sent at the specified {@link TimeValue}.
     * The pings that are sent carry a timeout of 1.25 times the specified {@link TimeValue}. When pinging each node, a connection and
     * handshake is performed, with a connection timeout of the specified {@link TimeValue}.
     *
     * @param resultsConsumer the callback when pinging is complete
     * @param duration        the timeout for various components of the pings
     */
    @Override
    public void ping(final Consumer<PingCollection> resultsConsumer, final TimeValue duration) {
        ping(resultsConsumer, duration, duration);
    }

    /**
     * a variant of {@link #ping(Consumer, TimeValue)}, but allows separating the scheduling duration
     * from the duration used for request level time outs. This is useful for testing
     */
    protected void ping(final Consumer<PingCollection> resultsConsumer,
                        final TimeValue scheduleDuration,
                        final TimeValue requestDuration) {
        final List<TransportAddress> seedAddresses = new ArrayList<>();
        seedAddresses.addAll(hostsProvider.buildDynamicHosts(createHostsResolver()));
        final DiscoveryNodes nodes = contextProvider.clusterState().nodes();
        // add all possible master nodes that were active in the last known cluster configuration
        for (ObjectCursor<DiscoveryNode> masterNode : nodes.getMasterNodes().values()) {
            seedAddresses.add(masterNode.value.getAddress());
        }

        final ConnectionProfile connectionProfile =
            ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG, requestDuration, requestDuration);
        final PingingRound pingingRound = new PingingRound(pingingRoundIdGenerator.incrementAndGet(), seedAddresses, resultsConsumer,
            nodes.getLocalNode(), connectionProfile);
        activePingingRounds.put(pingingRound.id(), pingingRound);
        final AbstractRunnable pingSender = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (e instanceof AlreadyClosedException == false) {
                    logger.warn("unexpected error while pinging", e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                sendPings(requestDuration, pingingRound);
            }
        };
        threadPool.generic().execute(pingSender);
        threadPool.schedule(TimeValue.timeValueMillis(scheduleDuration.millis() / 3), ThreadPool.Names.GENERIC, pingSender);
        threadPool.schedule(TimeValue.timeValueMillis(scheduleDuration.millis() / 3 * 2), ThreadPool.Names.GENERIC, pingSender);
        threadPool.schedule(scheduleDuration, ThreadPool.Names.GENERIC, new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                finishPingingRound(pingingRound);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected error while finishing pinging round", e);
            }
        });
    }

    // for testing
    protected void finishPingingRound(PingingRound pingingRound) {
        pingingRound.close();
    }

    protected class PingingRound implements Releasable {
        private final int id;
        private final Map<TransportAddress, Connection> tempConnections = new HashMap<>();
        private final KeyedLock<TransportAddress> connectionLock = new KeyedLock<>(true);
        private final PingCollection pingCollection;
        private final List<TransportAddress> seedAddresses;
        private final Consumer<PingCollection> pingListener;
        private final DiscoveryNode localNode;
        private final ConnectionProfile connectionProfile;

        private AtomicBoolean closed = new AtomicBoolean(false);

        PingingRound(int id, List<TransportAddress> seedAddresses, Consumer<PingCollection> resultsConsumer, DiscoveryNode localNode,
                     ConnectionProfile connectionProfile) {
            this.id = id;
            this.seedAddresses = Collections.unmodifiableList(seedAddresses.stream().distinct().collect(Collectors.toList()));
            this.pingListener = resultsConsumer;
            this.localNode = localNode;
            this.connectionProfile = connectionProfile;
            this.pingCollection = new PingCollection();
        }

        public int id() {
            return this.id;
        }

        public boolean isClosed() {
            return this.closed.get();
        }

        public List<TransportAddress> getSeedAddresses() {
            ensureOpen();
            return seedAddresses;
        }

        public Connection getOrConnect(DiscoveryNode node) throws IOException {
            Connection result;
            try (Releasable ignore = connectionLock.acquire(node.getAddress())) {
                result = tempConnections.get(node.getAddress());
                if (result == null) {
                    ensureOpen();
                    boolean success = false;
                    logger.trace("[{}] opening connection to [{}]", id(), node);
                    result = transportService.openConnection(node, connectionProfile);
                    try {
                        transportService.handshake(result, connectionProfile.getHandshakeTimeout().millis());
                        synchronized (this) {
                            // acquire lock and check if closed, to prevent leaving an open connection after closing
                            ensureOpen();
                            Connection existing = tempConnections.put(node.getAddress(), result);
                            assert existing == null;
                            success = true;
                        }
                    } finally {
                        if (success == false) {
                            logger.trace("[{}] closing connection to [{}] due to failure", id(), node);
                            IOUtils.closeWhileHandlingException(result);
                        }
                    }
                }
            }
            return result;
        }

        private void ensureOpen() {
            if (isClosed()) {
                throw new AlreadyClosedException("pinging round [" + id + "] is finished");
            }
        }

        public void addPingResponseToCollection(PingResponse pingResponse) {
            if (localNode.equals(pingResponse.node()) == false) {
                pingCollection.addPing(pingResponse);
            }
        }

        @Override
        public void close() {
            List<Connection> toClose = null;
            synchronized (this) {
                if (closed.compareAndSet(false, true)) {
                    activePingingRounds.remove(id);
                    toClose = new ArrayList<>(tempConnections.values());
                    tempConnections.clear();
                }
            }
            if (toClose != null) {
                // we actually closed
                try {
                    pingListener.accept(pingCollection);
                } finally {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        public ConnectionProfile getConnectionProfile() {
            return connectionProfile;
        }
    }


    protected void sendPings(final TimeValue timeout, final PingingRound pingingRound) {
        final ClusterState lastState = contextProvider.clusterState();
        final UnicastPingRequest pingRequest = new UnicastPingRequest(pingingRound.id(), timeout, createPingResponse(lastState));

        List<TransportAddress> temporalAddresses = temporalResponses.stream().map(pingResponse -> {
            assert clusterName.equals(pingResponse.clusterName()) :
                "got a ping request from a different cluster. expected " + clusterName + " got " + pingResponse.clusterName();
            return pingResponse.node().getAddress();
        }).collect(Collectors.toList());

        final Stream<TransportAddress> uniqueAddresses = Stream.concat(pingingRound.getSeedAddresses().stream(),
            temporalAddresses.stream()).distinct();

        // resolve what we can via the latest cluster state
        final Set<DiscoveryNode> nodesToPing = uniqueAddresses
            .map(address -> {
                DiscoveryNode foundNode = lastState.nodes().findByAddress(address);
                if (foundNode != null && transportService.nodeConnected(foundNode)) {
                    return foundNode;
                } else {
                    return new DiscoveryNode(
                        address.toString(),
                        address,
                        emptyMap(),
                        emptySet(),
                        Version.CURRENT.minimumCompatibilityVersion());
                }
            }).collect(Collectors.toSet());

        nodesToPing.forEach(node -> sendPingRequestToNode(node, timeout, pingingRound, pingRequest));
    }

    private void sendPingRequestToNode(final DiscoveryNode node, TimeValue timeout, final PingingRound pingingRound,
                                       final UnicastPingRequest pingRequest) {
        submitToExecutor(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                Connection connection = null;
                if (transportService.nodeConnected(node)) {
                    try {
                        // concurrency can still cause disconnects
                        connection = transportService.getConnection(node);
                    } catch (NodeNotConnectedException e) {
                        logger.trace("[{}] node [{}] just disconnected, will create a temp connection", pingingRound.id(), node);
                    }
                }

                if (connection == null) {
                    connection = pingingRound.getOrConnect(node);
                }

                logger.trace("[{}] sending to {}", pingingRound.id(), node);
                transportService.sendRequest(connection, ACTION_NAME, pingRequest,
                    TransportRequestOptions.builder().withTimeout((long) (timeout.millis() * 1.25)).build(),
                    getPingResponseHandler(pingingRound, node));
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ConnectTransportException || e instanceof AlreadyClosedException) {
                    // can't connect to the node - this is more common path!
                    logger.trace(() -> new ParameterizedMessage("[{}] failed to ping {}", pingingRound.id(), node), e);
                } else if (e instanceof RemoteTransportException) {
                    // something went wrong on the other side
                    logger.debug(() -> new ParameterizedMessage(
                            "[{}] received a remote error as a response to ping {}", pingingRound.id(), node), e);
                } else {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed send ping to {}", pingingRound.id(), node), e);
                }
            }

            @Override
            public void onRejection(Exception e) {
                // The RejectedExecutionException can come from the fact unicastZenPingExecutorService is at its max down in sendPings
                // But don't bail here, we can retry later on after the send ping has been scheduled.
                logger.debug("Ping execution rejected", e);
            }
        });
    }

    // for testing
    protected void submitToExecutor(AbstractRunnable abstractRunnable) {
        unicastZenPingExecutorService.execute(abstractRunnable);
    }

    // for testing
    protected TransportResponseHandler<UnicastPingResponse> getPingResponseHandler(final PingingRound pingingRound,
                                                                                   final DiscoveryNode node) {
        return new TransportResponseHandler<UnicastPingResponse>() {

            @Override
            public UnicastPingResponse read(StreamInput in) throws IOException {
                return new UnicastPingResponse(in);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleResponse(UnicastPingResponse response) {
                logger.trace("[{}] received response from {}: {}", pingingRound.id(), node, Arrays.toString(response.pingResponses));
                if (pingingRound.isClosed()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] skipping received response from {}. already closed", pingingRound.id(), node);
                    }
                } else {
                    Stream.of(response.pingResponses).forEach(pingingRound::addPingResponseToCollection);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException ||
                    exp.getCause() instanceof AlreadyClosedException) {
                    // ok, not connected...
                    logger.trace(() -> new ParameterizedMessage("failed to connect to {}", node), exp);
                } else if (closed == false) {
                    logger.warn(() -> new ParameterizedMessage("failed to send ping to [{}]", node), exp);
                }
            }
        };
    }

    private UnicastPingResponse handlePingRequest(final UnicastPingRequest request) {
        assert clusterName.equals(request.pingResponse.clusterName()) :
            "got a ping request from a different cluster. expected " + clusterName + " got " + request.pingResponse.clusterName();
        temporalResponses.add(request.pingResponse);
        // add to any ongoing pinging
        activePingingRounds.values().forEach(p -> p.addPingResponseToCollection(request.pingResponse));
        threadPool.schedule(TimeValue.timeValueMillis(request.timeout.millis() * 2), ThreadPool.Names.SAME,
            () -> temporalResponses.remove(request.pingResponse));

        List<PingResponse> pingResponses = CollectionUtils.iterableAsArrayList(temporalResponses);
        pingResponses.add(createPingResponse(contextProvider.clusterState()));

        return new UnicastPingResponse(request.id, pingResponses.toArray(new PingResponse[pingResponses.size()]));
    }

    class UnicastPingRequestHandler implements TransportRequestHandler<UnicastPingRequest> {

        @Override
        public void messageReceived(UnicastPingRequest request, TransportChannel channel) throws Exception {
            if (closed) {
                throw new AlreadyClosedException("node is shutting down");
            }
            if (request.pingResponse.clusterName().equals(clusterName)) {
                channel.sendResponse(handlePingRequest(request));
            } else {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "mismatched cluster names; request: [%s], local: [%s]",
                        request.pingResponse.clusterName().value(),
                        clusterName.value()));
            }
        }

    }

    static class UnicastPingRequest extends TransportRequest {

        final int id;
        final TimeValue timeout;
        final PingResponse pingResponse;

        UnicastPingRequest(int id, TimeValue timeout, PingResponse pingResponse) {
            this.id = id;
            this.timeout = timeout;
            this.pingResponse = pingResponse;
        }

        UnicastPingRequest(StreamInput in) throws IOException {
            super(in);
            id = in.readInt();
            timeout = in.readTimeValue();
            pingResponse = new PingResponse(in);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            out.writeTimeValue(timeout);
            pingResponse.writeTo(out);
        }
    }

    private PingResponse createPingResponse(ClusterState clusterState) {
        DiscoveryNodes discoNodes = clusterState.nodes();
        return new PingResponse(discoNodes.getLocalNode(), discoNodes.getMasterNode(), clusterState);
    }

    static class UnicastPingResponse extends TransportResponse {

        final int id;

        final PingResponse[] pingResponses;

        UnicastPingResponse(int id, PingResponse[] pingResponses) {
            this.id = id;
            this.pingResponses = pingResponses;
        }

        UnicastPingResponse(StreamInput in) throws IOException {
            id = in.readInt();
            pingResponses = new PingResponse[in.readVInt()];
            for (int i = 0; i < pingResponses.length; i++) {
                pingResponses[i] = new PingResponse(in);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            out.writeVInt(pingResponses.length);
            for (PingResponse pingResponse : pingResponses) {
                pingResponse.writeTo(out);
            }
        }
    }

    protected Version getVersion() {
        return Version.CURRENT; // for tests
    }
}
