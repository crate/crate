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

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.apache.lucene.util.LuceneTestCase.rarely;

/**
 * A basic transport implementation that allows to intercept requests that have been sent
 */
public class MockTransport implements Transport, LifecycleComponent {

    private volatile Map<String, RequestHandlerRegistry> requestHandlers = Collections.emptyMap();
    private final Object requestHandlerMutex = new Object();
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    private TransportMessageListener listener;
    private ConcurrentMap<Long, Tuple<DiscoveryNode, String>> requests = new ConcurrentHashMap<>();

    public TransportService createTransportService(Settings settings,
                                                   ThreadPool threadPool,
                                                   TransportInterceptor interceptor,
                                                   Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                                   @Nullable ClusterSettings clusterSettings) {
        StubbableConnectionManager connectionManager = new StubbableConnectionManager(new ConnectionManager(settings, this, threadPool),
            settings, this, threadPool);
        connectionManager.setDefaultNodeConnectedBehavior((cm, discoveryNode) -> nodeConnected(discoveryNode));
        connectionManager.setDefaultGetConnectionBehavior((cm, discoveryNode) -> openConnection(discoveryNode, null));
        return new TransportService(
            settings,
            this,
            threadPool,
            interceptor,
            localNodeFactory,
            clusterSettings,
            connectionManager
        );
    }

    /**
     * simulate a response for the given requestId
     */
    @SuppressWarnings("unchecked")
    public <Response extends TransportResponse> void handleResponse(final long requestId, final Response response) {
        final TransportResponseHandler<Response> transportResponseHandler =
            (TransportResponseHandler<Response>) responseHandlers.onResponseReceived(requestId, listener);
        if (transportResponseHandler != null) {
            final Response deliveredResponse;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                response.writeTo(output);
                deliveredResponse = transportResponseHandler.read(
                    new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writeableRegistry()));
            } catch (IOException | UnsupportedOperationException e) {
                throw new AssertionError("failed to serialize/deserialize response " + response, e);
            }
            transportResponseHandler.handleResponse(deliveredResponse);
        }
    }

    /**
     * simulate a local error for the given requestId, will be wrapped
     * by a {@link SendRequestTransportException}
     *
     * @param requestId the id corresponding to the captured send
     *                  request
     * @param t         the failure to wrap
     */
    public void handleLocalError(final long requestId, final Throwable t) {
        Tuple<DiscoveryNode, String> request = requests.get(requestId);
        assert request != null;
        this.handleError(requestId, new SendRequestTransportException(request.v1(), request.v2(), t));
    }

    /**
     * simulate a remote error for the given requestId, will be wrapped
     * by a {@link RemoteTransportException}
     *
     * @param requestId the id corresponding to the captured send
     *                  request
     * @param t         the failure to wrap
     */
    public void handleRemoteError(final long requestId, final Throwable t) {
        final RemoteTransportException remoteException;
        if (rarely(Randomness.get())) {
            remoteException = new RemoteTransportException("remote failure, coming from local node", t);
        } else {
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeException(t);
                remoteException = new RemoteTransportException("remote failure", output.bytes().streamInput().readException());
            } catch (IOException ioException) {
                throw new AssertionError("failed to serialize/deserialize supplied exception " + t, ioException);
            }
        }
        this.handleError(requestId, remoteException);
    }

    /**
     * simulate an error for the given requestId, unlike
     * {@link #handleLocalError(long, Throwable)} and
     * {@link #handleRemoteError(long, Throwable)}, the provided
     * exception will not be wrapped but will be delivered to the
     * transport layer as is
     *
     * @param requestId the id corresponding to the captured send
     *                  request
     * @param e         the failure
     */
    public void handleError(final long requestId, final TransportException e) {
        final TransportResponseHandler transportResponseHandler = responseHandlers.onResponseReceived(requestId, listener);
        if (transportResponseHandler != null) {
            transportResponseHandler.handleException(e);
        }
    }

    @Override
    public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) {
        return new CloseableConnection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {
                requests.put(requestId, Tuple.tuple(node, action));
                onSendRequest(requestId, action, request, node);
            }

            @Override
            public String toString() {
                return "MockTransportConnection{node=" + node + ", requests=" + requests + '}';
            }
        };
    }

    protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
    }

    protected boolean nodeConnected(DiscoveryNode discoveryNode) {
        return true;
    }

    @Override
    public TransportStats getStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address) {
        return new TransportAddress[0];
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void close() {
    }

    @Override
    public List<String> getDefaultSeedAddresses() {
        return Collections.emptyList();
    }

    @Override
    public <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            HashMap<String, RequestHandlerRegistry> copy = new HashMap<>(requestHandlers);
            copy.put(reg.getAction(), reg);
            requestHandlers = Map.copyOf(copy);
        }
    }

    @Override
    public ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RequestHandlerRegistry<TransportRequest> getRequestHandler(String action) {
        return requestHandlers.get(action);
    }

    @Override
    public void addMessageListener(TransportMessageListener listener) {
        if (this.listener != null) {
            throw new IllegalStateException("listener already set");
        }
        this.listener = listener;
    }

    @Override
    public boolean removeMessageListener(TransportMessageListener listener) {
        if (listener == this.listener) {
            this.listener = null;
            return true;
        }
        return false;
    }

    protected NamedWriteableRegistry writeableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }
}
