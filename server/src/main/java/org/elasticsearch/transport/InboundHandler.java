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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

public class InboundHandler {

    private static final Logger LOGGER = LogManager.getLogger(InboundHandler.class);

    private final ThreadPool threadPool;
    private final OutboundHandler outboundHandler;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;
    private final Transport.ResponseHandlers responseHandlers;
    private final Transport.RequestHandlers requestHandlers;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    // Empty stream constant to avoid instantiating a new stream for empty messages.
    private static final StreamInput EMPTY_STREAM_INPUT = new ByteBufferStreamInput(ByteBuffer.wrap(BytesRef.EMPTY_BYTES));

    InboundHandler(ThreadPool threadPool,
                   OutboundHandler outboundHandler,
                   NamedWriteableRegistry namedWriteableRegistry,
                   TransportHandshaker handshaker,
                   TransportKeepAlive keepAlive,
                   Transport.RequestHandlers requestHandlers,
                   Transport.ResponseHandlers responseHandlers) {
        this.threadPool = threadPool;
        this.outboundHandler = outboundHandler;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handshaker = handshaker;
        this.keepAlive = keepAlive;
        this.requestHandlers = requestHandlers;
        this.responseHandlers = responseHandlers;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.millis();
    }

    void inboundMessage(CloseableChannel channel, InboundMessage message) throws Exception {
        long startTime = threadPool.relativeTimeInMillis();
        channel.markAccessed(startTime);
        TransportLogger.logInboundMessage(channel, message);

        if (message.isPing()) {
            keepAlive.receiveKeepAlive(channel);
        } else {
            messageReceived(channel, message, startTime);
        }
    }

    private void messageReceived(CloseableChannel channel, InboundMessage message, long startTime) throws IOException {
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        assert header.needsToReadVariableHeader() == false;

        try {
            if (header.isRequest()) {
                handleRequest(channel, header, message);
            } else {
                // Responses do not support short circuiting currently
                assert message.isShortCircuit() == false;
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
                    final StreamInput streamInput;
                    if (message.getContentLength() > 0 || header.getVersion().equals(Version.CURRENT) == false) {
                        streamInput = namedWriteableStream(message.openOrGetStreamInput());
                        assertRemoteVersion(streamInput, header.getVersion());
                        if (header.isError()) {
                            handlerResponseError(streamInput, handler);
                        } else {
                            handleResponse(remoteAddress, streamInput, handler);
                        }
                        // Check the entire message has been read
                        final int nextByte = streamInput.read();
                        // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
                        if (nextByte != -1) {
                            throw new IllegalStateException("Message not fully read (response) for requestId ["
                                + requestId + "], handler [" + handler + "], error [" + header.isError()
                                + "]; resetting");
                        }
                    } else {
                        assert header.isError() == false;
                        handleResponse(remoteAddress, EMPTY_STREAM_INPUT, handler);
                    }
                }
            }
        } finally {
            final long took = threadPool.relativeTimeInMillis() - startTime;
            final long logThreshold = slowLogThresholdMs;
            if (logThreshold > 0 && took > logThreshold) {
                LOGGER.warn("handling inbound transport message [{}] took [{}ms] which is above the warn threshold of [{}ms]",
                    message, took, logThreshold);
            }
        }
    }

    private <T extends TransportRequest> void handleRequest(CloseableChannel channel, Header header, InboundMessage message) throws IOException {
        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final Version version = header.getVersion();
        if (header.isHandshake()) {
            messageListener.onRequestReceived(requestId, action);
            // Cannot short circuit handshakes
            assert message.isShortCircuit() == false;
            final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
            assertRemoteVersion(stream, header.getVersion());
            final TransportChannel transportChannel = new TcpTransportChannel(
                outboundHandler,
                channel,
                action,
                requestId,
                version,
                header.isCompressed(),
                header.isHandshake(),
                message.takeBreakerReleaseControl()
            );
            try {
                handshaker.handleHandshake(transportChannel, requestId, stream);
            } catch (Exception e) {
                if (Version.CURRENT.isCompatible(header.getVersion())) {
                    sendErrorResponse(action, transportChannel, e);
                } else {
                    LOGGER.warn(new ParameterizedMessage(
                        "could not send error response to handshake received on [{}] using wire format version [{}], closing channel",
                        channel, header.getVersion()), e);
                    channel.close();
                }
            }
        } else {
            final TransportChannel transportChannel = new TcpTransportChannel(outboundHandler, channel, action, requestId, version,
                header.isCompressed(), header.isHandshake(), message.takeBreakerReleaseControl());
            try {
                messageListener.onRequestReceived(requestId, action);
                if (message.isShortCircuit()) {
                    sendErrorResponse(action, transportChannel, message.getException());
                } else {
                    final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                    assertRemoteVersion(stream, header.getVersion());
                    final RequestHandlerRegistry<T> reg = requestHandlers.getHandler(action);
                    assert reg != null;
                    final T request = reg.newRequest(stream);
                    // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
                    final int nextByte = stream.read();
                    // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
                    if (nextByte != -1) {
                        throw new IllegalStateException("Message not fully read (request) for requestId [" + requestId + "], action ["
                            + action + "], available [" + stream.available() + "]; resetting");
                    }
                    final String executor = reg.getExecutor();
                    if (ThreadPool.Names.SAME.equals(executor)) {
                        try {
                            reg.processMessageReceived(request, transportChannel);
                        } catch (Exception e) {
                            sendErrorResponse(reg.getAction(), transportChannel, e);
                        }
                    } else {
                        threadPool.executor(executor).execute(new RequestHandler<>(reg, request, transportChannel));
                    }
                }
            } catch (Exception e) {
                sendErrorResponse(action, transportChannel, e);
            }

        }
    }

    private static void sendErrorResponse(String actionName, TransportChannel transportChannel, Exception e) {
        try {
            transportChannel.sendResponse(e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            LOGGER.warn(() -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", actionName), inner);
        }
    }

    private <T extends TransportResponse> void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream,
                                                              final TransportResponseHandler<T> handler) {
        final T response;
        try {
            response = handler.read(stream);
        } catch (Exception e) {
            final Exception serializationException = new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler + "]", e);
            LOGGER.warn(new ParameterizedMessage("Failed to deserialize response from [{}]", remoteAddress), serializationException);
            handleException(handler, serializationException);
            return;
        }
        final String executor = handler.executor();
        if (ThreadPool.Names.SAME.equals(executor)) {
            doHandleResponse(handler, response);
        } else {
            threadPool.executor(executor).execute(() -> doHandleResponse(handler, response));
        }
    }

    private <T extends TransportResponse> void doHandleResponse(TransportResponseHandler<T> handler, T response) {
        try {
            handler.handleResponse(response);
        } catch (Exception e) {
            handleException(handler, new ResponseHandlerFailureTransportException(e));
        }
    }

    private void handlerResponseError(StreamInput stream, final TransportResponseHandler<?> handler) {
        Exception error;
        try {
            error = stream.readException();
        } catch (Exception e) {
            error = new TransportSerializationException(
                    "Failed to deserialize exception response from stream for handler [" + handler + "]", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler<?> handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(new AbstractRunnable() {

            @Override
            protected void doRun() throws Exception {
                try {
                    handler.handleException(rtx);
                } catch (Exception e) {
                    LOGGER.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    handler.handleException(rtx);
                } catch (Exception err) {
                    e.addSuppressed(err);
                    LOGGER.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
                }
            }

            @Override
            public boolean isForceExecution() {
                return true;
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
            sendErrorResponse(reg.getAction(), transportChannel, e);
        }
    }
}
