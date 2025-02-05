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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Utils;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;


public final class OutboundHandler {

    private static final Logger LOGGER = LogManager.getLogger(OutboundHandler.class);

    private final String nodeName;
    private final Version version;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    OutboundHandler(String nodeName,
                    Version version,
                    StatsTracker statsTracker,
                    ThreadPool threadPool,
                    BigArrays bigArrays) {
        this.nodeName = nodeName;
        this.version = version;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
    }

    ChannelFuture sendBytes(CloseableChannel channel, byte[] bytes) {
        channel.markAccessed(threadPool.relativeTimeInMillis());
        try {
            ChannelFuture future = channel.writeAndFlush(Unpooled.wrappedBuffer(bytes));
            future.addListener(f -> {
                if (f.isSuccess()) {
                    statsTracker.incrementBytesSent(bytes.length);
                } else {
                    LOGGER.warn("send message failed [channel: {}]", channel, f.cause());
                }
            });
            return future;
        } catch (RuntimeException ex) {
            channel.close();
            throw ex;
        }
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    public void sendRequest(final DiscoveryNode node,
                            final CloseableChannel channel,
                            final long requestId,
                            final String action,
                            final TransportRequest request,
                            final TransportRequestOptions options,
                            final Version channelVersion,
                            final boolean compressRequest,
                            final boolean isHandshake) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        OutboundMessage.Request message = new OutboundMessage.Request(
            request,
            version,
            action,
            requestId,
            isHandshake,
            compressRequest
        );
        ChannelFuture future = sendMessage(channel, message);
        future.addListener(f -> messageListener.onRequestSent(node, requestId, action, request, options));
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     */
    void sendResponse(final Version nodeVersion,
                      final CloseableChannel channel,
                      final long requestId,
                      final String action,
                      final TransportResponse response,
                      final boolean compress,
                      final boolean isHandshake) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        OutboundMessage.Response message = new OutboundMessage.Response(
            response,
            version,
            requestId,
            isHandshake,
            compress
        );
        ChannelFuture future = sendMessage(channel, message);
        future.addListener(f -> messageListener.onResponseSent(requestId, action, response));
    }

    /**
     * Sends back an error response to the caller via the given channel
     */
    void sendErrorResponse(final Version nodeVersion,
                           final CloseableChannel channel,
                           final long requestId,
                           final String action,
                           final Exception error) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        TransportAddress address = new TransportAddress(channel.getLocalAddress());
        RemoteTransportException tx = new RemoteTransportException(nodeName, address, action, error);
        OutboundMessage.Response message = new OutboundMessage.Response(
            tx,
            version,
            requestId,
            false,
            false
        );
        ChannelFuture future = sendMessage(channel, message);
        future.addListener(f -> messageListener.onResponseSent(requestId, action, error));
    }

    private ChannelFuture sendMessage(CloseableChannel channel, OutboundMessage networkMessage) throws IOException {
        channel.markAccessed(threadPool.relativeTimeInMillis());

        var bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
        try {
            BytesReference msg = networkMessage.serialize(bytesStreamOutput);
            ChannelFuture future = channel.writeAndFlush(Netty4Utils.toByteBuf(msg));
            future.addListener(f -> {
                statsTracker.incrementBytesSent(msg.length());
                bytesStreamOutput.close();
                if (!f.isSuccess()) {
                    LOGGER.warn("send message failed [channel: {}]", channel, f.cause());
                }
            });
            return future;
        } catch (RuntimeException ex) {
            bytesStreamOutput.close();
            channel.close();
            throw ex;
        }
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }
}
