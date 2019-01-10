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

package org.elasticsearch.test.transport;

import com.carrotsearch.randomizedtesting.SysGlobals;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A mock delegate service that allows to simulate different network topology failures.
 * Internally it maps TransportAddress objects to rules that inject failures.
 * Adding rules for a node is done by adding rules for all bound addresses of a node
 * (and the publish address, if different).
 * Matching requests to rules is based on the delegate address associated with the
 * discovery node of the request, namely by DiscoveryNode.getAddress().
 * This address is usually the publish address of the node but can also be a different one
 * (for example, @see org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing, which constructs
 * fake DiscoveryNode instances where the publish address is one of the bound addresses).
 */
public final class MockTransportService extends TransportService {

    private final Map<DiscoveryNode, List<Transport.Connection>> openConnections = new HashMap<>();
    private static final int JVM_ORDINAL = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING);
        }
    }

    public static MockTransportService createNewService(Settings settings, Version version, ThreadPool threadPool,
                                                        @Nullable ClusterSettings clusterSettings) {
        MockTcpTransport mockTcpTransport = newMockTransport(settings, version, threadPool);
        return createNewService(settings, mockTcpTransport, version, threadPool, clusterSettings,
            Collections.emptySet());
    }

    public static MockTcpTransport newMockTransport(Settings settings, Version version, ThreadPool threadPool) {
        // some tests use MockTransportService to do network based testing. Yet, we run tests in multiple JVMs that means
        // concurrent tests could claim port that another JVM just released and if that test tries to simulate a disconnect it might
        // be smart enough to re-connect depending on what is tested. To reduce the risk, since this is very hard to debug we use
        // a different default port range per JVM unless the incoming settings override it
        int basePort = 10300 + (JVM_ORDINAL * 100); // use a non-default port otherwise some cluster in this JVM might reuse a port
        settings = Settings.builder().put(TcpTransport.PORT.getKey(), basePort + "-" + (basePort + 100)).put(settings).build();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        return new MockTcpTransport(settings, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()), version);
    }

    public static MockTransportService createNewService(Settings settings, Transport transport, Version version, ThreadPool threadPool,
                                                        @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders) {
        return new MockTransportService(settings, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress ->
                new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), UUIDs.randomBase64UUID(), boundAddress.publishAddress(),
                    Node.NODE_ATTRIBUTES.getAsMap(settings), DiscoveryNode.getRolesFromSettings(settings), version),
            clusterSettings, taskHeaders);
    }

    private final Transport original;

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor,
                                @Nullable ClusterSettings clusterSettings) {
        this(settings, transport, threadPool, interceptor, (boundAddress) ->
            DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), settings.get(Node.NODE_NAME_SETTING.getKey(),
                UUIDs.randomBase64UUID())), clusterSettings, Collections.emptySet());
    }

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor,
                                Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders) {
        this(settings, new StubbableTransport(transport), threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders);
    }

    private MockTransportService(Settings settings, StubbableTransport transport, ThreadPool threadPool, TransportInterceptor interceptor,
                                 Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                 @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders) {
        super(settings, transport, threadPool, interceptor, localNodeFactory, clusterSettings, taskHeaders,
            new StubbableConnectionManager(new ConnectionManager(settings, transport, threadPool), settings, transport, threadPool));
        this.original = transport.getDelegate();
    }

    public static TransportAddress[] extractTransportAddresses(TransportService transportService) {
        HashSet<TransportAddress> transportAddresses = new HashSet<>();
        BoundTransportAddress boundTransportAddress = transportService.boundAddress();
        transportAddresses.addAll(Arrays.asList(boundTransportAddress.boundAddresses()));
        transportAddresses.add(boundTransportAddress.publishAddress());
        return transportAddresses.toArray(new TransportAddress[transportAddresses.size()]);
    }

    @Override
    protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
            return new MockTaskManager(settings, threadPool, taskHeaders);
        } else {
            return super.createTaskManager(settings, threadPool, taskHeaders);
        }
    }

    /**
     * Clears all the registered rules.
     */
    public void clearAllRules() {
        transport().clearBehaviors();
        connectionManager().clearBehaviors();
    }

    /**
     * Clears the rule associated with the provided delegate service.
     */
    public void clearRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            clearRule(transportAddress);
        }
    }

    /**
     * Clears the rule associated with the provided delegate address.
     */
    public void clearRule(TransportAddress transportAddress) {
        transport().clearBehavior(transportAddress);
        connectionManager().clearBehavior(transportAddress);
    }

    /**
     * Adds a rule that will cause every send request to fail, and each new connect since the rule
     * is added to fail as well.
     */
    public void addFailToSendNoConnectRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress);
        }
    }

    /**
     * Adds a rule that will cause every send request to fail, and each new connect since the rule
     * is added to fail as well.
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress) {
        transport().addConnectBehavior(transportAddress, (transport, discoveryNode, profile) -> {
            throw new ConnectTransportException(discoveryNode, "DISCONNECT: simulated");
        });

        transport().addSendBehavior(transportAddress, (connection, requestId, action, request, options) -> {
            connection.close();
            // send the request, which will blow up
            connection.sendRequest(requestId, action, request, options);
        });
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportService transportService, final String... blockedActions) {
        addFailToSendNoConnectRule(transportService, new HashSet<>(Arrays.asList(blockedActions)));
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final String... blockedActions) {
        addFailToSendNoConnectRule(transportAddress, new HashSet<>(Arrays.asList(blockedActions)));
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportService transportService, final Set<String> blockedActions) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress, blockedActions);
        }
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final Set<String> blockedActions) {
        transport().addSendBehavior(transportAddress, (connection, requestId, action, request, options) -> {
            if (blockedActions.contains(action)) {
                logger.info("--> preventing {} request", action);
                connection.close();
            }
            connection.sendRequest(requestId, action, request, options);
        });
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     */
    public void addUnresponsiveRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addUnresponsiveRule(transportAddress);
        }
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     */
    public void addUnresponsiveRule(TransportAddress transportAddress) {
        transport().addConnectBehavior(transportAddress, (transport, discoveryNode, profile) -> {
            throw new ConnectTransportException(discoveryNode, "UNRESPONSIVE: simulated");
        });

        transport().addSendBehavior(transportAddress, (connection, requestId, action, request, options) -> {
            // don't send anything, the receiving node is unresponsive
        });
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     *
     * @param duration the amount of time to delay sending and connecting.
     */
    public void addUnresponsiveRule(TransportService transportService, final TimeValue duration) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addUnresponsiveRule(transportAddress, duration);
        }
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     *
     * @param duration the amount of time to delay sending and connecting.
     */
    public void addUnresponsiveRule(TransportAddress transportAddress, final TimeValue duration) {
        final long startTime = System.currentTimeMillis();

        Supplier<TimeValue> delaySupplier = () -> new TimeValue(duration.millis() - (System.currentTimeMillis() - startTime));

        transport().addConnectBehavior(transportAddress, (transport, discoveryNode, profile) -> {
            TimeValue delay = delaySupplier.get();
            if (delay.millis() <= 0) {
                return original.openConnection(discoveryNode, profile);
            }

            // TODO: Replace with proper setting
            TimeValue connectingTimeout = TransportService.TCP_CONNECT_TIMEOUT.getDefault(Settings.EMPTY);
            try {
                if (delay.millis() < connectingTimeout.millis()) {
                    Thread.sleep(delay.millis());
                    return original.openConnection(discoveryNode, profile);
                } else {
                    Thread.sleep(connectingTimeout.millis());
                    throw new ConnectTransportException(discoveryNode, "UNRESPONSIVE: simulated");
                }
            } catch (InterruptedException e) {
                throw new ConnectTransportException(discoveryNode, "UNRESPONSIVE: simulated");
            }
        });

        transport().addSendBehavior(transportAddress, new StubbableTransport.SendRequestBehavior() {
            private final Queue<Runnable> requestsToSendWhenCleared = new LinkedBlockingDeque<>();
            private boolean cleared = false;

            @Override
            public void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                    TransportRequestOptions options) throws IOException {
                // delayed sending - even if larger then the request timeout to simulated a potential late response from target node
                TimeValue delay = delaySupplier.get();
                if (delay.millis() <= 0) {
                    connection.sendRequest(requestId, action, request, options);
                    return;
                }

                // poor mans request cloning...
                RequestHandlerRegistry reg = MockTransportService.this.getRequestHandler(action);
                BytesStreamOutput bStream = new BytesStreamOutput();
                request.writeTo(bStream);
                final TransportRequest clonedRequest = reg.newRequest(bStream.bytes().streamInput());

                Runnable runnable = new AbstractRunnable() {
                    AtomicBoolean requestSent = new AtomicBoolean();

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("failed to send delayed request", e);
                    }

                    @Override
                    protected void doRun() throws IOException {
                        if (requestSent.compareAndSet(false, true)) {
                            connection.sendRequest(requestId, action, clonedRequest, options);
                        }
                    }
                };

                // store the request to send it once the rule is cleared.
                synchronized (this) {
                    if (cleared) {
                        runnable.run();
                    } else {
                        requestsToSendWhenCleared.add(runnable);
                        threadPool.schedule(delay, ThreadPool.Names.GENERIC, runnable);
                    }
                }
            }

            @Override
            public void clearCallback() {
                synchronized (this) {
                    assert cleared == false;
                    cleared = true;
                    requestsToSendWhenCleared.forEach(Runnable::run);
                }
            }
        });
    }

    /**
     * Adds a new send behavior that is used for communication with the given delegate service.
     *
     * @return {@code true} if no other send behavior was registered for any of the addresses bound by delegate service.
     */
    public boolean addSendBehavior(TransportService transportService, StubbableTransport.SendRequestBehavior sendBehavior) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addSendBehavior(transportAddress, sendBehavior);
        }
        return noRegistered;
    }

    /**
     * Adds a new send behavior that is used for communication with the given delegate address.
     *
     * @return {@code true} if no other send behavior was registered for this address before.
     */
    public boolean addSendBehavior(TransportAddress transportAddress, StubbableTransport.SendRequestBehavior sendBehavior) {
        return transport().addSendBehavior(transportAddress, sendBehavior);
    }

    /**
     * Adds a send behavior that is the default send behavior.
     *
     * @return {@code true} if no default send behavior was registered
     */
    public boolean addSendBehavior(StubbableTransport.SendRequestBehavior behavior) {
        return transport().setDefaultSendBehavior(behavior);
    }


    /**
     * Adds a new connect behavior that is used for creating connections with the given delegate service.
     *
     * @return {@code true} if no other send behavior was registered for any of the addresses bound by delegate service.
     */
    public boolean addConnectBehavior(TransportService transportService, StubbableTransport.OpenConnectionBehavior connectBehavior) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addConnectBehavior(transportAddress, connectBehavior);
        }
        return noRegistered;
    }

    /**
     * Adds a new connect behavior that is used for creating connections with the given delegate address.
     *
     * @return {@code true} if no other send behavior was registered for this address before.
     */
    public boolean addConnectBehavior(TransportAddress transportAddress, StubbableTransport.OpenConnectionBehavior connectBehavior) {
        return transport().addConnectBehavior(transportAddress, connectBehavior);
    }

    /**
     * Adds a new get connection behavior that is used for communication with the given delegate service.
     *
     * @return {@code true} if no other get connection behavior was registered for any of the addresses bound by delegate service.
     */
    public boolean addGetConnectionBehavior(TransportService transportService, StubbableConnectionManager.GetConnectionBehavior behavior) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addGetConnectionBehavior(transportAddress, behavior);
        }
        return noRegistered;
    }

    /**
     * Adds a get connection behavior that is used for communication with the given delegate address.
     *
     * @return {@code true} if no other get connection behavior was registered for this address before.
     */
    public boolean addGetConnectionBehavior(TransportAddress transportAddress, StubbableConnectionManager.GetConnectionBehavior behavior) {
        return connectionManager().addConnectBehavior(transportAddress, behavior);
    }

    /**
     * Adds a get connection behavior that is the default get connection behavior.
     *
     * @return {@code true} if no default get connection behavior was registered.
     */
    public boolean addGetConnectionBehavior(StubbableConnectionManager.GetConnectionBehavior behavior) {
        return connectionManager().setDefaultConnectBehavior(behavior);
    }

    /**
     * Adds a node connected behavior that is used for the given delegate service.
     *
     * @return {@code true} if no other node connected behavior was registered for any of the addresses bound by delegate service.
     */
    public boolean addNodeConnectedBehavior(TransportService transportService, StubbableConnectionManager.NodeConnectedBehavior behavior) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addNodeConnectedBehavior(transportAddress, behavior);
        }
        return noRegistered;
    }

    /**
     * Adds a node connected behavior that is used for the given delegate address.
     *
     * @return {@code true} if no other node connected behavior was registered for this address before.
     */
    public boolean addNodeConnectedBehavior(TransportAddress transportAddress, StubbableConnectionManager.NodeConnectedBehavior behavior) {
        return connectionManager().addNodeConnectedBehavior(transportAddress, behavior);
    }

    /**
     * Adds a node connected behavior that is the default node connected behavior.
     *
     * @return {@code true} if no default node connected behavior was registered.
     */
    public boolean addNodeConnectedBehavior(StubbableConnectionManager.NodeConnectedBehavior behavior) {
        return connectionManager().setDefaultNodeConnectedBehavior(behavior);
    }

    public StubbableTransport transport() {
        return (StubbableTransport) transport;
    }

    public StubbableConnectionManager connectionManager() {
        return (StubbableConnectionManager) connectionManager;
    }

    List<Tracer> activeTracers = new CopyOnWriteArrayList<>();

    public static class Tracer {
        public void receivedRequest(long requestId, String action) {
        }

        public void responseSent(long requestId, String action) {
        }

        public void responseSent(long requestId, String action, Throwable t) {
        }

        public void receivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        }

        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        }
    }

    public void addTracer(Tracer tracer) {
        activeTracers.add(tracer);
    }

    public boolean removeTracer(Tracer tracer) {
        return activeTracers.remove(tracer);
    }

    public void clearTracers() {
        activeTracers.clear();
    }

    @Override
    protected boolean traceEnabled() {
        return super.traceEnabled() || activeTracers.isEmpty() == false;
    }

    @Override
    protected void traceReceivedRequest(long requestId, String action) {
        super.traceReceivedRequest(requestId, action);
        for (Tracer tracer : activeTracers) {
            tracer.receivedRequest(requestId, action);
        }
    }

    @Override
    protected void traceResponseSent(long requestId, String action) {
        super.traceResponseSent(requestId, action);
        for (Tracer tracer : activeTracers) {
            tracer.responseSent(requestId, action);
        }
    }

    @Override
    protected void traceResponseSent(long requestId, String action, Exception e) {
        super.traceResponseSent(requestId, action, e);
        for (Tracer tracer : activeTracers) {
            tracer.responseSent(requestId, action, e);
        }
    }

    @Override
    protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        super.traceReceivedResponse(requestId, sourceNode, action);
        for (Tracer tracer : activeTracers) {
            tracer.receivedResponse(requestId, sourceNode, action);
        }
    }

    @Override
    protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        super.traceRequestSent(node, requestId, action, options);
        for (Tracer tracer : activeTracers) {
            tracer.requestSent(node, requestId, action, options);
        }
    }

    public Transport getOriginalTransport() {
        Transport transport = transport();
        while (transport instanceof StubbableTransport) {
            transport = ((StubbableTransport) transport).getDelegate();
        }
        return transport;
    }

    @Override
    public Transport.Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
        Transport.Connection connection = super.openConnection(node, profile);

        synchronized (openConnections) {
            List<Transport.Connection> connections = openConnections.computeIfAbsent(node,
                (n) -> new CopyOnWriteArrayList<>());
            connections.add(connection);
        }

        connection.addCloseListener(ActionListener.wrap(() -> {
            synchronized (openConnections) {
                List<Transport.Connection> connections = openConnections.get(node);
                boolean remove = connections.remove(connection);
                assert remove : "Should have removed connection";
                if (connections.isEmpty()) {
                    openConnections.remove(node);
                }
            }
        }));

        return connection;
    }

    @Override
    protected void doClose() throws IOException {
        super.doClose();
        synchronized (openConnections) {
            assert openConnections.size() == 0 : "still open connections: " + openConnections;
        }
    }

    public DiscoveryNode getLocalDiscoNode() {
        return this.getLocalNode();
    }

}
