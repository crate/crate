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

package org.elasticsearch.transport;

import static org.elasticsearch.common.settings.Setting.timeSetting;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public class TransportService extends AbstractLifecycleComponent implements TransportMessageListener, TransportConnectionListener {

    protected static final Logger LOGGER = LogManager.getLogger(TransportService.class);

    public static final Setting<TimeValue> TCP_CONNECT_TIMEOUT =
        timeSetting("transport.tcp.connect_timeout", NetworkService.TCP_CONNECT_TIMEOUT, Setting.Property.NodeScope);
    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final AtomicBoolean handleIncomingRequests = new AtomicBoolean();
    protected final Transport transport;
    protected final ConnectionManager connectionManager;
    protected final ThreadPool threadPool;
    protected final ClusterName clusterName;
    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;
    private final Transport.ResponseHandlers responseHandlers;

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    private final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers =
        Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, TimeoutInfoHolder> eldest) {
                return size() > 100;
            }
        });


    private final Logger tracerLog;

    /** if set will call requests sent to this id to shortcut and executed locally */
    volatile DiscoveryNode localNode = null;
    private final Transport.Connection localNodeConnection = new Transport.Connection() {
        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
            sendLocalRequest(requestId, action, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    };

    /**
     * Build the service.
     *
     * @param clusterSettings if non null, the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *    updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING} and {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
     */
    public TransportService(Settings settings,
                            Transport transport,
                            ThreadPool threadPool,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                            @Nullable ClusterSettings clusterSettings) {
        this(
            settings,
            transport,
            threadPool,
            localNodeFactory,
            clusterSettings,
            new ClusterConnectionManager(settings, transport)
        );
    }

    public TransportService(Settings settings,
                            Transport transport,
                            ThreadPool threadPool,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                            @Nullable ClusterSettings clusterSettings,
                            ConnectionManager connectionManager) {
        this.transport = transport;
        transport.setSlowLogThreshold(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.get(settings));
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        tracerLog = Loggers.getLogger(LOGGER, ".tracer");
        responseHandlers = transport.getResponseHandlers();
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING, transport::setSlowLogThreshold);
        }

        registerRequestHandler(
            HANDSHAKE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            HandshakeRequest::new,
            (request, channel) -> channel.sendResponse(
                new HandshakeResponse(localNode, clusterName, localNode.getVersion())));
    }



    public DiscoveryNode getLocalNode() {
        return localNode;
    }


    /**
     * The executor service for this transport service.
     *
     * @return the executor service
     */
    private Executor getExecutorService() {
        return threadPool.generic();
    }

    @Override
    protected void doStart() {
        transport.setMessageListener(this);
        connectionManager.addListener(this);
        transport.start();
        if (transport.boundAddress() != null && LOGGER.isInfoEnabled()) {
            LOGGER.info("{}", transport.boundAddress());
        }
        localNode = localNodeFactory.apply(transport.boundAddress());
    }

    @Override
    protected void doStop() {
        try {
            IOUtils.close(connectionManager, transport::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (final Transport.ResponseContext<?> holderToNotify : responseHandlers.prune(h -> true)) {
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                getExecutorService().execute(new AbstractRunnable() {

                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        LOGGER.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                holderToNotify.action()),
                            e);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                holderToNotify.action()),
                            e);
                    }

                    @Override
                    public void doRun() {
                        TransportException ex = new SendRequestTransportException(holderToNotify.connection().getNode(),
                            holderToNotify.action(), new NodeClosedException(localNode));
                        holderToNotify.handler().handleException(ex);
                    }
                });
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        transport.close();
    }

    /**
     * start accepting incoming requests.
     * when the transport layer starts up it will block any incoming requests until
     * this method is called
     */
    public final void acceptIncomingRequests() {
        handleIncomingRequests.set(true);
    }

    public TransportStats stats() {
        return transport.getStats();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getDefaultSeedAddresses() {
        return transport.getDefaultSeedAddresses();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || connectionManager.nodeConnected(node);
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node the node to connect to
     * @param listener the action listener to notify
     */
    public void connectToNode(DiscoveryNode node, ActionListener<Void> listener) throws ConnectTransportException {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        connectionManager.connectToNode(node, connectionValidator(node), listener);
    }

    public ConnectionManager.ConnectionValidator connectionValidator(DiscoveryNode node) {
        return (newConnection, actualProfile, listener) -> {
            // We don't validate cluster names to allow for CCS connections.
            handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true, listener.map(resp -> {
                final DiscoveryNode remote = resp.discoveryNode;
                if (node.equals(remote) == false) {
                    throw new ConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
                }
                return null;
            }));
        };
    }

    /**
     * Establishes a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     * @param listener the action listener to notify
     */
    public void openConnection(final DiscoveryNode node, ConnectionProfile connectionProfile,
                               ActionListener<Transport.Connection> listener) {
        if (isLocalNode(node)) {
            listener.onResponse(localNodeConnection);
        } else {
            connectionManager.openConnection(node, connectionProfile, listener);
        }
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node mismatches the local cluster name.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param listener         action listener to notify
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    public void handshake(
        final Transport.Connection connection,
        final long handshakeTimeout,
        final ActionListener<DiscoveryNode> listener) {
        handshake(connection, handshakeTimeout, clusterName.getEqualityPredicate(),
            listener.map(HandshakeResponse::getDiscoveryNode));
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node doesn't match the local cluster name.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param clusterNamePredicate cluster name validation predicate
     * @param listener         action listener to notify
     * @return the handshake response
     * @throws IllegalStateException if the handshake failed
     */
    public void handshake(final Transport.Connection connection,
                          final long handshakeTimeout,
                          final Predicate<ClusterName> clusterNamePredicate,
                          final ActionListener<HandshakeResponse> listener) {
        final DiscoveryNode node = connection.getNode();
        sendRequest(
            connection,
            HANDSHAKE_ACTION_NAME,
            HandshakeRequest.INSTANCE,
            TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(),
            new ActionListenerResponseHandler<>(
                new ActionListener<>() {

                    @Override
                    public void onResponse(HandshakeResponse response) {
                        if (clusterNamePredicate.test(response.clusterName) == false) {
                            listener.onFailure(new IllegalStateException("handshake with [" + node + "] failed: remote cluster name ["
                                + response.clusterName.value() + "] does not match " + clusterNamePredicate));
                        } else if (response.version.isCompatible(localNode.getVersion()) == false) {
                            listener.onFailure(new IllegalStateException("handshake with [" + node + "] failed: remote node version ["
                                + response.version + "] is incompatible with local node version [" + localNode.getVersion() + "]"));
                        } else {
                            listener.onResponse(response);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                },
                HandshakeResponse::new,
                ThreadPool.Names.GENERIC
            ));
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    static class HandshakeRequest extends TransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        private HandshakeRequest() {
        }

        public HandshakeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class HandshakeResponse extends TransportResponse {

        private final DiscoveryNode discoveryNode;
        private final ClusterName clusterName;
        private final Version version;

        public HandshakeResponse(DiscoveryNode discoveryNode, ClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        public HandshakeResponse(StreamInput in) throws IOException {
            discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
            clusterName = new ClusterName(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(discoveryNode);
            clusterName.writeTo(out);
            Version.writeVersion(version, out);
        }

        public DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionManager.addListener(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionManager.removeListener(listener);
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                          final TransportRequest request,
                                                          final TransportResponseHandler<T> handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public final <T extends TransportResponse> void sendRequest(final DiscoveryNode node,
                                                                final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, options, handler);
    }

    /**
     * Sends a request on the specified connection. If there is a failure sending the request, the specified handler is invoked.
     *
     * @param connection the connection to send the request on
     * @param action     the name of the action
     * @param request    the request
     * @param options    the options for this request
     * @param handler    the response handler
     * @param <T>        the type of the transport response
     */
    public final <T extends TransportResponse> void sendRequest(final Transport.Connection connection,
                                                                final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                final TransportResponseHandler<T> handler) {
        try {

            final TransportResponseHandler<T> delegate = new TransportResponseHandler<T>() {
                @Override
                public T read(StreamInput in) throws IOException {
                    return handler.read(in);
                }

                @Override
                public void handleResponse(T response) {
                    handler.handleResponse(response);

                }

                @Override
                public void handleException(TransportException exp) {
                    handler.handleException(exp);
                }

                @Override
                public String executor() {
                    return handler.executor();
                }

                @Override
                public String toString() {
                    return getClass().getName() + "/[" + action + "]:" + handler.toString();
                }
            };

            sendRequestInternal(connection, action, request, options, delegate);
        } catch (final Exception ex) {
            // the caller might not handle this so we invoke the handler
            final TransportException te;
            if (ex instanceof TransportException) {
                te = (TransportException) ex;
            } else {
                te = new TransportException("failure to send", ex);
            }
            handler.handleException(te);
        }
    }

    /**
     * Returns either a real transport connection or a local node connection if we are using the local node optimization.
     * @throws NodeNotConnectedException if the given node is not connected
     */
    public Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.getConnection(node);
        }
    }

    public final <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node,
                                                                     final String action,
                                                                     final TransportRequest request,
                                                                     final TransportRequestOptions options,
                                                                     final TransportResponseHandler<T> handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendChildRequest(connection, action, request, options, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection,
                                                               final String action,
                                                               final TransportRequest request,
                                                               final TransportResponseHandler<T> handler) {
        sendChildRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection,
                                                               final String action,
                                                               final TransportRequest request,
                                                               final TransportRequestOptions options,
                                                               final TransportResponseHandler<T> handler) {
        sendRequest(connection, action, request, options, handler);
    }

    private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection,
                                                                   final String action,
                                                                   final TransportRequest request,
                                                                   final TransportRequestOptions options,
                                                                   TransportResponseHandler<T> handler) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();
        if (lifecycle.stoppedOrClosed()) {
            handler.handleException(new SendRequestTransportException(node, action, new NodeClosedException(localNode)));
            return;
        }
        final long requestId = responseHandlers.newRequestId();
        final TimeoutHandler timeoutHandler;
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action, options.timeout());
        } else {
            timeoutHandler = null;
        }
        TimeoutResponseHandler<T> responseHandler = new TimeoutResponseHandler<>(handler, timeoutHandler);
        responseHandlers.add(requestId, new Transport.ResponseContext<>(responseHandler, connection, action));
        if (timeoutHandler != null) {
            timeoutHandler.schedule();
        }
        try {
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final Transport.ResponseContext<? extends TransportResponse> contextToNotify = responseHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (contextToNotify == null) {
                LOGGER.debug("Exception while sending request, handler likely already notified due to timeout", e);
                return;
            }

            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows. In the special case of running into a closing node we run on the current
            // thread on a best effort basis though.
            final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
            final String executor = lifecycle.stoppedOrClosed() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
            threadPool.executor(executor).execute(new AbstractRunnable() {
                @Override
                public void onRejection(Exception e) {
                    // if we get rejected during node shutdown we don't wanna bubble it up
                    LOGGER.debug(
                        () -> new ParameterizedMessage(
                            "failed to notify response handler on rejection, action: {}",
                            contextToNotify.action()),
                        e);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.warn(
                        () -> new ParameterizedMessage(
                            "failed to notify response handler on exception, action: {}",
                            contextToNotify.action()),
                        e);
                }

                @Override
                protected void doRun() throws Exception {
                    contextToNotify.handler().handleException(sendRequestException);
                }
            });
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(LOGGER, localNode, action, requestId, this, threadPool);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            final RequestHandlerRegistry reg = getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                //noinspection unchecked
                reg.processMessageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        //noinspection unchecked
                        reg.processMessageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            LOGGER.warn(() -> new ParameterizedMessage(
                                    "failed to notify channel of error message for action [{}]", action), inner);
                        }
                    }

                    @Override
                    public String toString() {
                        return "processing of [" + requestId + "][" + action + "]: " + request;
                    }
                });
            }

        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                LOGGER.warn(
                    () -> new ParameterizedMessage(
                        "failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return transport.addressesFromString(address);
    }

    /**
     * A set of all valid action prefixes.
     */
    public static final Set<String> VALID_ACTION_PREFIXES = Set.of(
            "indices:admin",
            "indices:monitor",
            "indices:data/write",
            "indices:data/read",
            "indices:internal",
            "cluster:admin",
            "cluster:monitor",
            "cluster:internal",
            "internal:");

    private void validateActionName(String actionName) {
        assert isValidActionName(actionName)
            : "invalid action name [" + actionName + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES;
    }

    /**
     * Returns <code>true</code> iff the action name starts with a valid prefix.
     *
     * @see #VALID_ACTION_PREFIXES
     */
    public static boolean isValidActionName(String actionName) {
        for (String prefix : VALID_ACTION_PREFIXES) {
            if (actionName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Registers a new request handler
     *
     * @param action         The action the request handler is associated with
     * @param requestReader  a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action,
                                                                          String executor,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        validateActionName(action);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, handler, executor, false, true);
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param requestReader               The request class that will be used to construct new instances for streaming
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action,
                                                                          String executor,
                                                                          boolean forceExecution,
                                                                          boolean canTripCircuitBreaker,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        validateActionName(action);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, handler, executor, forceExecution, canTripCircuitBreaker);
        transport.registerRequestHandler(reg);
    }

    /**
     * called by the {@link Transport} implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     */
    @Override
    public void onRequestReceived(long requestId, String action) {
        if (handleIncomingRequests.get() == false) {
            throw new IllegalStateException("transport not ready yet to handle incoming requests");
        }
        if (tracerLog.isTraceEnabled()) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }
    }

    /** called by the {@link Transport} implementation once a request has been sent */
    @Override
    public void onRequestSent(DiscoveryNode node,
                              long requestId,
                              String action,
                              TransportRequest request,
                              TransportRequestOptions options) {
        if (tracerLog.isTraceEnabled()) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }
    }

    @Override
    public void onResponseReceived(long requestId, Transport.ResponseContext<?> holder) {
        if (holder == null) {
            checkForTimeout(requestId);
        } else if (tracerLog.isTraceEnabled()) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, holder.action(), holder.connection().getNode());
        }
    }

    /** called by the {@link Transport} implementation once a response was sent to calling node */
    public void onResponseSent(long requestId, String action, TransportResponse response) {
        if (tracerLog.isTraceEnabled()) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }
    }

    /** called by the {@link Transport} implementation after an exception was sent as a response to an incoming request */
    @Override
    public void onResponseSent(long requestId, String action, Exception e) {
        if (tracerLog.isTraceEnabled()) {
            tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
        }
    }

    public RequestHandlerRegistry<? extends TransportRequest> getRequestHandler(String action) {
        return transport.getRequestHandlers().getHandler(action);
    }

    private void checkForTimeout(long requestId) {
        // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
        final DiscoveryNode sourceNode;
        final String action;
        assert responseHandlers.contains(requestId) == false;
        TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
        if (timeoutInfoHolder != null) {
            long time = threadPool.relativeTimeInMillis();
            LOGGER.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, " +
                    "action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(),
                timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
            action = timeoutInfoHolder.action();
            sourceNode = timeoutInfoHolder.node();
        } else {
            LOGGER.warn("Transport response handler not found of id [{}]", requestId);
            action = null;
            sourceNode = null;
        }
        // call tracer out of lock
        if (tracerLog.isTraceEnabled() == false) {
            return;
        }
        if (action == null) {
            assert sourceNode == null;
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        } else {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }
    }

    @Override
    public void onConnectionClosed(Transport.Connection connection) {
        try {
            List<Transport.ResponseContext<?>> pruned = responseHandlers.prune(h -> h.connection().getCacheKey().equals(connection
                .getCacheKey()));
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows
            getExecutorService().execute(() -> {
                for (Transport.ResponseContext<?> holderToNotify : pruned) {
                    holderToNotify.handler().handleException(new NodeDisconnectedException(connection.getNode(), holderToNotify.action()));
                }
            });
        } catch (EsRejectedExecutionException ex) {
            LOGGER.debug("Rejected execution on onConnectionClosed", ex);
        }
    }

    final class TimeoutHandler implements Runnable {

        private final long requestId;
        private final long sentTime = threadPool.relativeTimeInMillis();
        private final String action;
        private final DiscoveryNode node;
        private final TimeValue timeout;
        private Scheduler.Cancellable cancellable;

        TimeoutHandler(long requestId, DiscoveryNode node, String action, TimeValue timeout) {
            this.requestId = requestId;
            this.node = node;
            this.action = action;
            this.timeout = timeout;
        }

        void schedule() {
            this.cancellable = threadPool.schedule(this, timeout, ThreadPool.Names.GENERIC);
        }

        @Override
        public void run() {
            if (responseHandlers.contains(requestId)) {
                long timeoutTime = threadPool.relativeTimeInMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(node, action, sentTime, timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final Transport.ResponseContext<? extends TransportResponse> holder = responseHandlers.remove(requestId);
                if (holder != null) {
                    assert holder.action().equals(action);
                    assert holder.connection().getNode().equals(node);
                    holder.handler().handleException(
                        new ReceiveTimeoutTransportException(holder.connection().getNode(), holder.action(),
                                                             "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * cancels timeout handling. this is a best effort only to avoid running it. remove the requestId from {@link #responseHandlers}
         * to make sure this doesn't run.
         */
        public void cancel() {
            assert responseHandlers.contains(requestId) == false :
                "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public String toString() {
            return "timeout handler for [" + requestId + "][" + action + "]";
        }
    }

    static class TimeoutInfoHolder {

        private final DiscoveryNode node;
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the handle methods
     * are invoked we restore the context.
     */
    public static final class TimeoutResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final TransportResponseHandler<T> delegate;
        @Nullable
        private final TimeoutHandler handler;

        public TimeoutResponseHandler(TransportResponseHandler<T> delegate, @Nullable TimeoutHandler handler) {
            this.delegate = delegate;
            this.handler = handler;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            if (handler != null) {
                handler.cancel();
            }
            delegate.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            if (handler != null) {
                handler.cancel();
            }
            delegate.handleException(exp);
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }
    }

    static class DirectResponseChannel implements TransportChannel {

        final Logger logger;
        final DiscoveryNode localNode;
        private final String action;
        private final long requestId;
        final TransportService service;
        final ThreadPool threadPool;

        DirectResponseChannel(Logger logger, DiscoveryNode localNode, String action, long requestId, TransportService service,
                              ThreadPool threadPool) {
            this.logger = logger;
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            service.onResponseSent(requestId, action, response);
            final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            processResponse(handler, response);
                        }

                        @Override
                        public String toString() {
                            return "delivery of response to [" + requestId + "][" + action + "]: " + response;
                        }
                    });
                }
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        protected void processResponse(TransportResponseHandler handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            service.onResponseSent(requestId, action, exception);
            final TransportResponseHandler<?> handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(new Runnable() {
                        @Override
                        public void run() {
                            processException(handler, rtx);
                        }

                        @Override
                        public String toString() {
                            return "delivery of failure response to [" + requestId + "][" + action + "]: " + exception;
                        }
                    });
                }
            }
        }

        protected RemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final TransportResponseHandler<?> handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "failed to handle exception for action [{}], handler [{}]", action, handler), e);
            }
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }


    /**
     * Returns the internal thread pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }
}
