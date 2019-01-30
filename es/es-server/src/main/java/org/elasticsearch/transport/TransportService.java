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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public class TransportService extends AbstractLifecycleComponent implements TransportMessageListener, TransportConnectionListener {

    public static final Setting<Integer> CONNECTIONS_PER_NODE_RECOVERY =
        intSetting("transport.connections_per_node.recovery", 2, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_BULK =
        intSetting("transport.connections_per_node.bulk", 3, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_REG =
        intSetting("transport.connections_per_node.reg", 6, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_STATE =
        intSetting("transport.connections_per_node.state", 1, 1, Setting.Property.NodeScope);
    public static final Setting<Integer> CONNECTIONS_PER_NODE_PING =
        intSetting("transport.connections_per_node.ping", 1, 1, Setting.Property.NodeScope);
    public static final Setting<TimeValue> TCP_CONNECT_TIMEOUT =
        timeSetting("transport.tcp.connect_timeout", NetworkService.TCP_CONNECT_TIMEOUT, Setting.Property.NodeScope);
    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final CountDownLatch blockIncomingRequestsLatch = new CountDownLatch(1);
    protected final Transport transport;
    protected final ConnectionManager connectionManager;
    protected final ThreadPool threadPool;
    protected final ClusterName clusterName;
    protected final TaskManager taskManager;
    private final TransportInterceptor.AsyncSender asyncSender;
    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;
    private final boolean connectToRemoteCluster;
    private final Transport.ResponseHandlers responseHandlers;
    private final TransportInterceptor interceptor;

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers =
        Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > 100;
            }
        });

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {};

    // tracer log

    public static final Setting<List<String>> TRACE_LOG_INCLUDE_SETTING =
        listSetting("transport.tracer.include", emptyList(), Function.identity(), Property.Dynamic, Property.NodeScope);
    public static final Setting<List<String>> TRACE_LOG_EXCLUDE_SETTING =
        listSetting("transport.tracer.exclude", Arrays.asList("internal:discovery/zen/fd*", TransportLivenessAction.NAME),
            Function.identity(), Property.Dynamic, Property.NodeScope);

    private final Logger tracerLog;

    volatile String[] tracerLogInclude;
    volatile String[] tracerLogExclude;

    private final RemoteClusterService remoteClusterService;

    private final boolean validateConnections;

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
 *        updates for {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings,
                            Set<String> taskHeaders) {
        this(settings, transport, threadPool, transportInterceptor, localNodeFactory, clusterSettings, taskHeaders,
            new ConnectionManager(settings, transport, threadPool));
    }

    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings,
                            Set<String> taskHeaders, ConnectionManager connectionManager) {
        super(settings);
        // The only time we do not want to validate node connections is when this is a transport client using the simple node sampler
        this.validateConnections = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey())) == false ||
            TransportClient.CLIENT_TRANSPORT_SNIFF.get(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        taskManager = createTaskManager(settings, threadPool, taskHeaders);
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.connectToRemoteCluster = RemoteClusterService.ENABLE_REMOTE_CLUSTERS.get(settings);
        remoteClusterService = new RemoteClusterService(settings, this);
        responseHandlers = transport.getResponseHandlers();
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
            if (connectToRemoteCluster) {
                remoteClusterService.listenForUpdates(clusterSettings);
            }
        }
        registerRequestHandler(
            HANDSHAKE_ACTION_NAME,
            () -> HandshakeRequest.INSTANCE,
            ThreadPool.Names.SAME,
            false, false,
            (request, channel) -> channel.sendResponse(
                new HandshakeResponse(localNode, clusterName, localNode.getVersion())));
    }

    public RemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        return new TaskManager(settings, threadPool, taskHeaders);
    }

    /**
     * The executor service for this transport service.
     *
     * @return the executor service
     */
    private ExecutorService getExecutorService() {
        return threadPool.generic();
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() {
        transport.addMessageListener(this);
        connectionManager.addListener(this);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());
        if (connectToRemoteCluster) {
            // here we start to connect to the remote clusters
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {
        try {
            IOUtils.close(connectionManager, remoteClusterService, transport::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (final Transport.ResponseContext holderToNotify : responseHandlers.prune(h -> true)) {
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                getExecutorService().execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                holderToNotify.action()),
                            e);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                holderToNotify.action()),
                            e);
                    }
                    @Override
                    public void doRun() {
                        TransportException ex = new TransportException("transport stopped, action: " + holderToNotify.action());
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
        blockIncomingRequestsLatch.countDown();
    }

    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new TransportInfo(boundTransportAddress, transport.profileBoundAddresses());
    }

    public TransportStats stats() {
        return transport.getStats();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getLocalAddresses() {
        return transport.getLocalAddresses();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || connectionManager.nodeConnected(node);
    }

    /**
     * Connect to the specified node with the default connection profile
     *
     * @param node the node to connect to
     */
    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        connectToNode(node, null);
    }

    /**
     * Connect to the specified node with the given connection profile
     *
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     */
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.connectToNode(node, connectionProfile, connectionValidator(node));
    }

    public CheckedBiConsumer<Transport.Connection, ConnectionProfile, IOException> connectionValidator(DiscoveryNode node) {
        return (newConnection, actualProfile) -> {
            // We don't validate cluster names to allow for CCS connections.
            final DiscoveryNode remote = handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true).discoveryNode;
            if (validateConnections && node.equals(remote) == false) {
                throw new ConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
            }
        };

    }

    /**
     * Establishes and returns a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     */
    public Transport.Connection openConnection(final DiscoveryNode node, ConnectionProfile connectionProfile) throws IOException {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.openConnection(node, connectionProfile);
        }
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node mismatches the local cluster name.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @return the connected node
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    public DiscoveryNode handshake(
            final Transport.Connection connection,
            final long handshakeTimeout) throws ConnectTransportException {
        return handshake(connection, handshakeTimeout, clusterName::equals).discoveryNode;
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node doesn't match the local cluster name.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param clusterNamePredicate cluster name validation predicate
     * @return the handshake response
     * @throws IllegalStateException if the handshake failed
     */
    public HandshakeResponse handshake(
        final Transport.Connection connection,
        final long handshakeTimeout, Predicate<ClusterName> clusterNamePredicate) {
        final HandshakeResponse response;
        final DiscoveryNode node = connection.getNode();
        try {
            PlainTransportFuture<HandshakeResponse> futureHandler = new PlainTransportFuture<>(
                new FutureTransportResponseHandler<HandshakeResponse>() {
                @Override
                public HandshakeResponse newInstance() {
                    return new HandshakeResponse();
                }
            });
            sendRequest(connection, HANDSHAKE_ACTION_NAME, HandshakeRequest.INSTANCE,
                TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(), futureHandler);
            response = futureHandler.txGet();
        } catch (Exception e) {
            throw new IllegalStateException("handshake failed with " + node, e);
        }

        if (!clusterNamePredicate.test(response.clusterName)) {
            throw new IllegalStateException("handshake failed, mismatched cluster name [" + response.clusterName + "] - " + node);
        } else if (response.version.isCompatible(localNode.getVersion()) == false) {
            throw new IllegalStateException("handshake failed, incompatible version [" + response.version + "] - " + node);
        }

        return response;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    static class HandshakeRequest extends TransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        private HandshakeRequest() {
        }

    }

    public static class HandshakeResponse extends TransportResponse {
        private DiscoveryNode discoveryNode;
        private ClusterName clusterName;
        private Version version;

        HandshakeResponse() {
        }

        public HandshakeResponse(DiscoveryNode discoveryNode, ClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
            clusterName = new ClusterName(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportResponseHandler<T> handler) throws TransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportRequestOptions options,
                                                                          TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, futureHandler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            futureHandler.handleException(ex);
        }
        return futureHandler;
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                                final TransportRequest request,
                                                                final TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }
    }

    public final <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }
    }

    public final <T extends TransportResponse> void sendRequest(final Transport.Connection connection, final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {
        try {
            asyncSender.sendRequest(connection, action, request, options, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
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

    public final <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node, final String action,
                                                                     final TransportRequest request, final Task parentTask,
                                                                     final TransportRequestOptions options,
                                                                     final TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendChildRequest(connection, action, request, parentTask, options, handler);
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action,
                                                               final TransportRequest request, final Task parentTask,
                                                               final TransportResponseHandler<T> handler) {
        sendChildRequest(connection, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action,
                                                               final TransportRequest request, final Task parentTask,
                                                               final TransportRequestOptions options,
                                                               final TransportResponseHandler<T> handler) {
        request.setParentTask(localNode.getId(), parentTask.getId());
        try {
            sendRequest(connection, action, request, options, handler);
        } catch (TaskCancelledException ex) {
            // The parent task is already cancelled - just fail the request
            handler.handleException(new TransportException(ex));
        } catch (NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
        }

    }

    private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection, final String action,
                                                                   final TransportRequest request,
                                                                   final TransportRequestOptions options,
                                                                   TransportResponseHandler<T> handler) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();

        Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        ContextRestoreResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
        // TODO we can probably fold this entire request ID dance into connection.sendReqeust but it will be a bigger refactoring
        final long requestId = responseHandlers.add(new Transport.ResponseContext<>(responseHandler, connection, action));
        final TimeoutHandler timeoutHandler;
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
            responseHandler.setTimeoutHandler(timeoutHandler);
        } else {
            timeoutHandler = null;
        }
        try {
            if (lifecycle.stoppedOrClosed()) {
                // if we are not started the exception handling will remove the RequestHolder again and calls the handler to notify
                // the caller. It will only notify if the toStop code hasn't done the work yet.
                throw new TransportException("TransportService is closed stopped can't send request");
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);
            }
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final Transport.ResponseContext contextToNotify = responseHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (contextToNotify != null) {
                if (timeoutHandler != null) {
                    timeoutHandler.cancel();
                }
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                contextToNotify.action()),
                            e);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
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
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(logger, localNode, action, requestId, this, threadPool);
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
                            logger.warn(() -> new ParameterizedMessage(
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
                logger.warn(
                    () -> new ParameterizedMessage(
                        "failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    private boolean shouldTraceAction(String action) {
        if (tracerLogInclude.length > 0) {
            if (Regex.simpleMatch(tracerLogInclude, action) == false) {
                return false;
            }
        }
        if (tracerLogExclude.length > 0) {
            return !Regex.simpleMatch(tracerLogExclude, action);
        }
        return true;
    }

    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return transport.addressesFromString(address, perAddressLimit);
    }

    /**
     * A set of all valid action prefixes.
     */
    public static final Set<String> VALID_ACTION_PREFIXES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        "indices:admin",
        "indices:monitor",
        "indices:data/write",
        "indices:data/read",
        "indices:internal",
        "cluster:admin",
        "cluster:monitor",
        "cluster:internal",
        "internal:"
        )));

    private void validateActionName(String actionName) {
        // TODO we should makes this a hard validation and throw an exception but we need a good way to add backwards layer
        // for it. Maybe start with a deprecation layer
        if (isValidActionName(actionName) == false) {
            logger.warn("invalid action name [" + actionName + "] must start with one of: " +
                TransportService.VALID_ACTION_PREFIXES );
        }
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
     * @param requestFactory a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory,
                                                    String executor, TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, Streamable.newWriteableReader(requestFactory), taskManager, handler, executor, false, true);
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action         The action the request handler is associated with
     * @param requestReader  a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, String executor,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, taskManager, handler, executor, false, true);
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param request               The request class that will be used to construct new instances for streaming
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request,
                                                                          String executor, boolean forceExecution,
                                                                          boolean canTripCircuitBreaker,
                                                                          TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, Streamable.newWriteableReader(request), taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
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
                                                                          String executor, boolean forceExecution,
                                                                          boolean canTripCircuitBreaker,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        transport.registerRequestHandler(reg);
    }

    /** called by the {@link Transport} implementation once a request has been sent */
    public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                              TransportRequestOptions options) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceRequestSent(node, requestId, action, options);
        }
    }

    protected boolean traceEnabled() {
        return tracerLog.isTraceEnabled();
    }

    /** called by the {@link Transport} implementation once a response was sent to calling node */
    public void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions options) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceResponseSent(requestId, action);
        }
    }

    /** called by the {@link Transport} implementation after an exception was sent as a response to an incoming request */
    public void onResponseSent(long requestId, String action, Exception e) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceResponseSent(requestId, action, e);
        }
    }

    protected void traceResponseSent(long requestId, String action, Exception e) {
        tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
    }

    /**
     * called by the {@link Transport} implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     */
    public void onRequestReceived(long requestId, String action) {
        try {
            blockIncomingRequestsLatch.await();
        } catch (InterruptedException e) {
            logger.trace("interrupted while waiting for incoming requests block to be removed");
        }
        if (traceEnabled() && shouldTraceAction(action)) {
            traceReceivedRequest(requestId, action);
        }
    }

    public RequestHandlerRegistry getRequestHandler(String action) {
        return transport.getRequestHandler(action);
    }


    @Override
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        if (holder == null) {
            checkForTimeout(requestId);
        } else if (traceEnabled() && shouldTraceAction(holder.action())) {
            traceReceivedResponse(requestId, holder.connection().getNode(), holder.action());
        }
    }

    private void checkForTimeout(long requestId) {
        // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
        final DiscoveryNode sourceNode;
        final String action;
        assert responseHandlers.contains(requestId) == false;
        TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
        if (timeoutInfoHolder != null) {
            long time = threadPool.relativeTimeInMillis();
            logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, " +
                    "action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(),
                timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
            action = timeoutInfoHolder.action();
            sourceNode = timeoutInfoHolder.node();
        } else {
            logger.warn("Transport response handler not found of id [{}]", requestId);
            action = null;
            sourceNode = null;
        }
        // call tracer out of lock
        if (traceEnabled() == false) {
            return;
        }
        if (action == null) {
            assert sourceNode == null;
            traceUnresolvedResponse(requestId);
        } else if (shouldTraceAction(action)) {
            traceReceivedResponse(requestId, sourceNode, action);
        }
    }

    @Override
    public void onConnectionClosed(Transport.Connection connection) {
        try {
            List<Transport.ResponseContext> pruned = responseHandlers.prune(h -> h.connection().getCacheKey().equals(connection
                .getCacheKey()));
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows
            getExecutorService().execute(() -> {
                for (Transport.ResponseContext holderToNotify : pruned) {
                    holderToNotify.handler().handleException(new NodeDisconnectedException(connection.getNode(), holderToNotify.action()));
                }
            });
        } catch (EsRejectedExecutionException ex) {
            logger.debug("Rejected execution on onConnectionClosed", ex);
        }
    }

    protected void traceReceivedRequest(long requestId, String action) {
        tracerLog.trace("[{}][{}] received request", requestId, action);
    }

    protected void traceResponseSent(long requestId, String action) {
        tracerLog.trace("[{}][{}] sent response", requestId, action);
    }

    protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
    }

    protected void traceUnresolvedResponse(long requestId) {
        tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
    }

    protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
    }

    final class TimeoutHandler implements Runnable {

        private final long requestId;
        private final long sentTime = threadPool.relativeTimeInMillis();
        private final String action;
        private final DiscoveryNode node;
        volatile ScheduledFuture future;

        TimeoutHandler(long requestId, DiscoveryNode node, String action) {
            this.requestId = requestId;
            this.node = node;
            this.action = action;
        }

        @Override
        public void run() {
            if (responseHandlers.contains(requestId)) {
                long timeoutTime = threadPool.relativeTimeInMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(node, action, sentTime, timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final Transport.ResponseContext holder = responseHandlers.remove(requestId);
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
            FutureUtils.cancel(future);
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
    public static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final TransportResponseHandler<T> delegate;
        private final Supplier<ThreadContext.StoredContext> contextSupplier;
        private volatile TimeoutHandler handler;

        public ContextRestoreResponseHandler(Supplier<ThreadContext.StoredContext> contextSupplier, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            if(handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            if(handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

        void setTimeoutHandler(TimeoutHandler handler) {
            this.handler = handler;
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
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            sendResponse(response, TransportResponseOptions.EMPTY);
        }

        @Override
        public void sendResponse(final TransportResponse response, TransportResponseOptions options) throws IOException {
            service.onResponseSent(requestId, action, response, options);
            final TransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
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

        @SuppressWarnings("unchecked")
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
            final TransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
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

        protected void processException(final TransportResponseHandler handler, final RemoteTransportException rtx) {
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
