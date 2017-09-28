/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

/**
 * The handler for dealing with Postgres SSLRequest messages.
 *
 * ______________SslRequest_______________
 * | length (int32) |   payload (int32)  |  <- pattern
 * |       8        |      80877103      |  <- actual message
 *
 * https://www.postgresql.org/docs/current/static/protocol-flow.html#AEN113116
 *
 * The handler implementation should be stateless.
 */
public final class SslReqHandler {

    private static final Logger log = Loggers.getLogger(SslReqHandler.class);
    /** The total size of the SslRequest message (including the size itself) */
    static final int SSL_REQUEST_BYTE_LENGTH = 8;
    /* The payload of the SSL Request message */
    static final int SSL_REQUEST_CODE = 80877103;

    enum State {
        WAITING_FOR_INPUT,
        DONE
    }

    private final SslContext sslContext;

    SslReqHandler(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
     * Process receives incoming data from the Netty pipeline. It
     * may request more data by returning the WAITING_FOR_INPUT
     * state. The process method should return DONE when it has
     * finished processing. It may add additional elements to the
     * pipeline. The handler is responsible for to position the
     * buffer read marker correctly such that successive readers
     * see the correct data. The handler is expected to position the
     * marker after the SSLRequest payload.
     * @param buffer The buffer with incoming data
     * @param pipeline The Netty pipeline which may be modified
     * @return The state of the handler
     */
    public State process(ByteBuf buffer, ChannelPipeline pipeline) {
        if (buffer.readableBytes() < SSL_REQUEST_BYTE_LENGTH) {
            return State.WAITING_FOR_INPUT;
        }
        // mark the buffer so we can jump back if we don't handle this startup
        buffer.markReaderIndex();
        // reads the total message length (int) and the SSL request code (int)
        if (buffer.readInt() == SSL_REQUEST_BYTE_LENGTH && buffer.readInt() == SSL_REQUEST_CODE) {
            // received optional SSL negotiation pkg
            if (sslContext != null) {
                writeByteAndFlushMessage(pipeline.channel(), 'S');
                SslHandler sslHandler = sslContext.newHandler(pipeline.channel().alloc());
                pipeline.addFirst(sslHandler);
            } else {
                writeByteAndFlushMessage(pipeline.channel(), 'N');
            }
            buffer.markReaderIndex();
        } else {
            buffer.resetReaderIndex();
        }
        return State.DONE;
    }

    /**
     * Response to send to the remote end, either 'N' or 'S'.
     * @param channel The channel to write the response to
     * @param byteToWrite byte represented as an int
     */
    private static void writeByteAndFlushMessage(Channel channel, int byteToWrite) {
        ByteBuf channelBuffer = channel.alloc().buffer(1);
        channelBuffer.writeByte(byteToWrite);
        channel.writeAndFlush(channelBuffer);
    }
}
