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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.CheckedSupplier;
import io.crate.common.io.IOUtils;

final class OutboundHandler {

    private static final Logger LOGGER = LogManager.getLogger(OutboundHandler.class);

    private final MeanMetric transmittedBytesMetric = new MeanMetric();
    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    OutboundHandler(String nodeName,
                    Version version,
                    String[] features,
                    ThreadPool threadPool,
                    BigArrays bigArrays) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
    }

    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        SendContext sendContext = new SendContext(channel, () -> bytes, listener);
        try {
            internalSend(channel, sendContext);
        } catch (IOException e) {
            // This should not happen as the bytes are already serialized
            throw new AssertionError(e);
        }
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    void sendRequest(final DiscoveryNode node,
                     final TcpChannel channel,
                     final long requestId,
                     final String action,
                     final TransportRequest request,
                     final TransportRequestOptions options,
                     final Version channelVersion,
                     final boolean compressRequest,
                     final boolean isHandshake) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        OutboundMessage.Request message = new OutboundMessage.Request(
            features,
            request,
            version,
            action,
            requestId,
            isHandshake,
            compressRequest
        );
        ActionListener<Void> listener = ActionListener.wrap(() ->
            messageListener.onRequestSent(node, requestId, action, request, options));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse(Version, Set, TcpChannel, long, String, Exception) for sending error responses
     */
    void sendResponse(final Version nodeVersion,
                      final Set<String> features,
                      final TcpChannel channel,
                      final long requestId,
                      final String action,
                      final TransportResponse response,
                      final boolean compress,
                      final boolean isHandshake) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        OutboundMessage.Response message = new OutboundMessage.Response(
            features,
            response,
            version,
            requestId,
            isHandshake,
            compress
        );
        TransportResponseOptions options = TransportResponseOptions.builder().withCompress(compress).build();
        ActionListener<Void> listener = ActionListener.wrap(
            () -> messageListener.onResponseSent(requestId, action, response, options)
        );
        sendMessage(channel, message, listener);
    }

    /**
     * Sends back an error response to the caller via the given channel
     */
    void sendErrorResponse(final Version nodeVersion,
                           final Set<String> features,
                           final TcpChannel channel,
                           final long requestId,
                           final String action,
                           final Exception error) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        TransportAddress address = new TransportAddress(channel.getLocalAddress());
        RemoteTransportException tx = new RemoteTransportException(nodeName, address, action, error);
        OutboundMessage.Response message = new OutboundMessage.Response(
            features,
            tx,
            version,
            requestId,
            false,
            false
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        sendMessage(channel, message, listener);
    }

    private void sendMessage(TcpChannel channel, OutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        MessageSerializer serializer = new MessageSerializer(networkMessage, bigArrays);
        SendContext sendContext = new SendContext(channel, serializer, listener, serializer);
        internalSend(channel, sendContext);
    }

    /**
     * sends a message to the given channel, using the given callbacks.
     */
    private void internalSend(TcpChannel channel, SendContext sendContext) throws IOException {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        BytesReference reference = sendContext.get();
        try {
            channel.sendMessage(reference, sendContext);
        } catch (RuntimeException ex) {
            sendContext.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }

    }

    MeanMetric getTransmittedBytes() {
        return transmittedBytesMetric;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private final OutboundMessage message;
        private final BigArrays bigArrays;
        private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(OutboundMessage message, BigArrays bigArrays) {
            this.message = message;
            this.bigArrays = bigArrays;
        }

        @Override
        public BytesReference get() throws IOException {
            bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            return message.serialize(bytesStreamOutput);
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }

    private class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {

        private final TcpChannel channel;
        private final CheckedSupplier<BytesReference, IOException> messageSupplier;
        private final ActionListener<Void> listener;
        private final Releasable optionalReleasable;
        private long messageSize = -1;

        private SendContext(TcpChannel channel,
                            CheckedSupplier<BytesReference, IOException> messageSupplier,
                            ActionListener<Void> listener) {
            this(channel, messageSupplier, listener, null);
        }

        private SendContext(TcpChannel channel,
                            CheckedSupplier<BytesReference, IOException> messageSupplier,
                            ActionListener<Void> listener,
                            Releasable optionalReleasable) {
            this.channel = channel;
            this.messageSupplier = messageSupplier;
            this.listener = listener;
            this.optionalReleasable = optionalReleasable;
        }

        public BytesReference get() throws IOException {
            BytesReference message;
            try {
                message = messageSupplier.get();
                messageSize = message.length();
                return message;
            } catch (Exception e) {
                onFailure(e);
                throw e;
            }
        }

        @Override
        protected void innerOnResponse(Void v) {
            assert messageSize != -1 : "If onResponse is being called, the message should have been serialized";
            transmittedBytesMetric.inc(messageSize);
            closeAndCallback(() -> listener.onResponse(v));
        }

        @Override
        protected void innerOnFailure(Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            closeAndCallback(() -> listener.onFailure(e));
        }

        private void closeAndCallback(Runnable runnable) {
            Releasables.close(optionalReleasable, runnable::run);
        }
    }
}
