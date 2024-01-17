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

package org.elasticsearch.transport.netty4;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transports;

import io.crate.common.exceptions.Exceptions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final Netty4Transport transport;
    private final InboundPipeline pipeline;

    public Netty4MessageChannelHandler(PageCacheRecycler recycler, Netty4Transport transport) {
        this.transport = transport;
        final ThreadPool threadPool = transport.getThreadPool();
        final Transport.RequestHandlers requestHandlers = transport.getRequestHandlers();
        this.pipeline = new InboundPipeline(
            transport.getVersion(),
            transport.getStatsTracker(),
            recycler,
            threadPool::relativeTimeInMillis,
            transport.getInflightBreaker(),
            requestHandlers::getHandler,
            transport::inboundMessage
        );
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert Transports.assertTransportThread();
        assert msg instanceof ByteBuf : "Expected message type ByteBuf, found: " + msg.getClass();

        final ByteBuf buffer = (ByteBuf) msg;
        CloseableChannel channel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        final BytesReference wrapped = Netty4Utils.toBytesReference(buffer);
        try (ReleasableBytesReference reference = new ReleasableBytesReference(wrapped, buffer::release)) {
            pipeline.handleBytes(channel, reference);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        final Throwable throwable = cause;
        Exceptions.maybeDieOnAnotherThread(throwable);
        final Throwable unwrapped = Exceptions.unwrap(cause, ElasticsearchException.class);
        final Throwable newCause = unwrapped != null ? unwrapped : cause;
        CloseableChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (newCause instanceof Error) {
            transport.onException(tcpChannel, new Exception(newCause));
        } else {
            transport.onException(tcpChannel, (Exception) newCause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Releasables.closeIgnoringException(pipeline);
        super.channelInactive(ctx);
    }
}
