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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.logging.log4j.Logger;

import io.crate.auth.Protocol;
import io.crate.protocols.ssl.ConnectionTest;
import io.crate.protocols.ssl.SslContextProvider;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

/**
 * Part of the logic from this file is from
 *
 * https://github.com/opendistro-for-elasticsearch/security/blob/main/src/main/java/com/amazon/opendistroforelasticsearch/security/ssl/transport/DualModeSSLHandler.java
 **/
public final class DualProbeAwareHandler extends ByteToMessageDecoder {

    public static final String NAME = "dual_probe_aware_handler";
    private final SslContextProvider sslContextProvider;
    private final Logger logger;
    private final boolean plainSupported;

    public DualProbeAwareHandler(Logger logger, SslContextProvider sslContextProvider, boolean plainSupported) {
        this.logger = logger;
        this.sslContextProvider = sslContextProvider;
        this.plainSupported = plainSupported;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Will use the first six bytes to detect a protocol.
        if (in.readableBytes() < 6) {
            return;
        }
        int offset = in.readerIndex();
        if (in.getCharSequence(offset, 6, StandardCharsets.UTF_8).equals(ConnectionTest.DUAL_MODE_CLIENT_HELLO_MSG)) {
            in.readerIndex(in.readerIndex() + 6);
            logger.debug("Received DualSSL Client Hello message");
            ByteBuf responseBuffer = ctx.alloc().buffer(6);
            responseBuffer.writeCharSequence(ConnectionTest.DUAL_MODE_SERVER_HELLO_MSG, StandardCharsets.UTF_8);
            ctx.writeAndFlush(responseBuffer).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        boolean isEncrypted = SslHandler.isEncrypted(in);
        if (isEncrypted) {
            logger.debug("Identified request as SSL request");
            SslContext sslContext = sslContextProvider.getServerContext(Protocol.TRANSPORT);
            SslHandler sslHandler = sslContext.newHandler(ctx.alloc());
            ctx.pipeline().addAfter(NAME, "ssl_server", sslHandler);
            ctx.pipeline().remove(this);
            logger.debug("Replaced DualModeSSLHandler with SSLHandler");
        } else if (plainSupported) {
            logger.debug("Identified request as non SSL request, running without SSL dual mode is enabled");
            ctx.pipeline().remove(this);
        } else {
            // Data is not encrypted but node supports only SSL/TLS.
            // Discard everything and close the connection.
            in.clear();
            ctx.close();
        }
    }
}

