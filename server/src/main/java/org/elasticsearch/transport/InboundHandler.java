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

import io.crate.common.collections.MapBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

public class InboundHandler {

    private static final Logger LOGGER = LogManager.getLogger(InboundHandler.class);

    private final MeanMetric readBytesMetric = new MeanMetric();
    private final ThreadPool threadPool;
    private final OutboundHandler outboundHandler;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CircuitBreakerService circuitBreakerService;
    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;

    private final Transport.ResponseHandlers responseHandlers = new Transport.ResponseHandlers();
    private volatile Map<String, RequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    InboundHandler(ThreadPool threadPool,
                   OutboundHandler outboundHandler,
                   NamedWriteableRegistry namedWriteableRegistry,
                   CircuitBreakerService circuitBreakerService,
                   TransportHandshaker handshaker,
                   TransportKeepAlive keepAlive) {
        this.threadPool = threadPool;
        this.outboundHandler = outboundHandler;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.handshaker = handshaker;
        this.keepAlive = keepAlive;
    }

    synchronized <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        if (requestHandlers.containsKey(reg.getAction())) {
            throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
        }
        requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
    }

    @SuppressWarnings("unchecked")
    final <T extends TransportRequest> RequestHandlerRegistry<T> getRequestHandler(String action) {
        return (RequestHandlerRegistry<T>) requestHandlers.get(action);
    }

    final Transport.ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    MeanMetric getReadBytes() {
        return readBytesMetric;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    void inboundMessage(TcpChannel channel, InboundMessage message) throws Exception {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        TransportLogger.logInboundMessage(channel, message);

        if (message.isPing()) {
            readBytesMetric.inc(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            keepAlive.receiveKeepAlive(channel);
        } else {
            readBytesMetric.inc(message.getHeader().getNetworkMessageSize() + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            messageReceived(message, channel);
        }
    }

    void handleDecodeException(TcpChannel channel, Header header) {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        readBytesMetric.inc(header.getNetworkMessageSize() + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
    }

    private void messageReceived(InboundMessage message, TcpChannel channel) throws IOException {
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        assert header.needsToReadVariableHeader() == false;

        final StreamInput streamInput = namedWriteableStream(message.openOrGetStreamInput());
        assertRemoteVersion(streamInput, header.getVersion());

        if (header.isRequest()) {
            handleRequest(channel, header, streamInput, message.getContentLength());
        } else {
            final TransportResponseHandler<?> handler;
            long requestId = header.getRequestId();
            if (header.isHandshake()) {
                handler = handshaker.removeHandlerForHandshake(requestId);
            } else {
                TransportResponseHandler<? extends TransportResponse> theHandler =
                    responseHandlers.onResponseReceived(requestId, messageListener);
                if (theHandler == null && header.isError()) {
                    handler = handshaker.removeHandlerForHandshake(requestId);
                } else {
                    handler = theHandler;
                }
            }
            // ignore if its null, the service logs it
            if (handler != null) {
                if (header.isError()) {
                    handlerResponseError(streamInput, handler);
                } else {
                    handleResponse(remoteAddress, streamInput, handler);
                }
                // Check the entire message has been read
                final int nextByte = streamInput.read();
                // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
                if (nextByte != -1) {
                    throw new IllegalStateException("Message not fully read (response) for requestId [" + requestId + "], handler ["
                        + handler + "], error [" + header.isError() + "]; resetting");
                }
            }
        }
    }

    private <T extends TransportRequest> void handleRequest(TcpChannel channel, Header header, StreamInput stream, int messageLengthBytes) {
        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final Version version = header.getVersion();
        TransportChannel transportChannel = null;
        try {
            messageListener.onRequestReceived(requestId, action);
            if (header.isHandshake()) {
                handshaker.handleHandshake(version, channel, requestId, stream);
            } else {
                final RequestHandlerRegistry<T> reg = getRequestHandler(action);
                if (reg == null) {
                    throw new ActionNotFoundTransportException(action);
                }
                CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
                if (reg.canTripCircuitBreaker()) {
                    breaker.addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
                } else {
                    breaker.addWithoutBreaking(messageLengthBytes);
                }
                transportChannel = new TcpTransportChannel(
                    outboundHandler,
                    channel,
                    action,
                    requestId,
                    version,
                    circuitBreakerService,
                    messageLengthBytes,
                    header.isCompressed()
                );
                final T request = reg.newRequest(stream);

                // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
                final int nextByte = stream.read();
                // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
                if (nextByte != -1) {
                    throw new IllegalStateException("Message not fully read (request) for requestId [" + requestId + "], action [" + action
                        + "], available [" + stream.available() + "]; resetting");
                }
                threadPool.executor(reg.getExecutor()).execute(new RequestHandler<>(reg, request, transportChannel));
            }
        } catch (Exception e) {
            // the circuit breaker tripped
            if (transportChannel == null) {
                transportChannel = new TcpTransportChannel(outboundHandler, channel, action, requestId, version,
                    circuitBreakerService, 0, header.isCompressed());
            }
            try {
                transportChannel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                LOGGER.warn(() -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", action), inner);
            }
        }
    }

    private <T extends TransportResponse> void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream,
                                                              final TransportResponseHandler<T> handler) {
        final T response;
        try {
            response = handler.read(stream);
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler.getClass().getName() + "]", e));
            return;
        }
        threadPool.executor(handler.executor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }

            @Override
            protected void doRun() {
                handler.handleResponse(response);
            }
        });
    }

    private void handlerResponseError(StreamInput stream, final TransportResponseHandler handler) {
        Exception error;
        try {
            error = stream.readException();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(() -> {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                LOGGER.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
            }
        });
    }

    private StreamInput namedWriteableStream(StreamInput delegate) {
        return new NamedWriteableAwareStreamInput(delegate, namedWriteableRegistry);
    }

    static void assertRemoteVersion(StreamInput in, Version version) {
        assert version.equals(in.getVersion()) : "Stream version [" + in.getVersion() + "] does not match version [" + version + "]";
    }

    private static class RequestHandler<T extends TransportRequest> extends AbstractRunnable {
        private final RequestHandlerRegistry<T> reg;
        private final T request;
        private final TransportChannel transportChannel;

        RequestHandler(RequestHandlerRegistry<T> reg, T request, TransportChannel transportChannel) {
            this.reg = reg;
            this.request = request;
            this.transportChannel = transportChannel;
        }

        @Override
        protected void doRun() throws Exception {
            reg.processMessageReceived(request, transportChannel);
        }

        @Override
        public boolean isForceExecution() {
            return reg.isForceExecution();
        }

        @Override
        public void onFailure(Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                LOGGER.warn(() -> new ParameterizedMessage(
                    "Failed to send error message back to client for action [{}]", reg.getAction()), inner);
            }
        }
    }
}
