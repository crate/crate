/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.OptionalSslHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import org.apache.commons.math3.analysis.function.Log;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * This class is hybrid of {@link SslHandler} and {@link OptionalSslHandler}
 * It behaves like SslHandler for outgoing messages and encrypted incoming messages
 * and behaves like OptionalSslHandler for unencrypted incoming messages.
 */
public class AdaptiveSslHandler extends SslHandler {

    private static final Logger LOGGER = LogManager.getLogger(AdaptiveSslHandler.class);
    private static final int SSL_RECORD_HEADER_LENGTH = 5;

    public AdaptiveSslHandler(SSLEngine engine) {
        super(engine);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws SSLException {
        // Inspired by OptionalSslHandler.decode.

        if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
            return;
        }

        if (SslHandler.isEncrypted(in)) {
            // forward to SslHandler's implementation to handle encrypted incoming message.
            super.decode(ctx, in, out);
        } else {
            // Plaintext is coming from a remote peer (server)
            // as it downgraded SSL to plaintext (based on HBA config).
            // AdaptiveSslHandler is not needed anymore.
                LOGGER.info("node {} got a plaintext response from {}",
                    ((InetSocketAddress) ctx.channel().localAddress()).getHostName(),
                    ((InetSocketAddress) ctx.channel().remoteAddress()).getHostName()
                );
            ctx.pipeline().remove(this);
        }
    }
}
