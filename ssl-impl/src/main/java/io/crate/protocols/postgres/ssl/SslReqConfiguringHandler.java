/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.protocols.postgres.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * Handler which configures SSL when it receives a PSQL SSLRequest.
 */
public class SslReqConfiguringHandler implements SslReqHandler {

    private final Logger LOGGER;
    private final SslContext sslContext;

    @SuppressWarnings("unused")
    public SslReqConfiguringHandler(Settings settings) {
        this(settings, SslConfiguration.buildSslContext(settings));
    }

    public SslReqConfiguringHandler(Settings settings, SslContext sslContext) {
        this.LOGGER = Loggers.getLogger(SslReqRejectingHandler.class, settings);
        this.sslContext = sslContext;
        assert this.sslContext != null : "Supplied context must never be null";
        LOGGER.info("SSL support is enabled.");
    }

    @Override
    public State process(ByteBuf buffer, ChannelPipeline pipeline) {
        if (buffer.readableBytes() < SSL_REQUEST_BYTE_LENGTH) {
            return State.WAITING_FOR_INPUT;
        }
        // mark the buffer so we can jump back if we don't handle this message
        buffer.markReaderIndex();
        // reads the total message length (int) and the SSL request code (int)
        if (buffer.readInt() == SSL_REQUEST_BYTE_LENGTH && buffer.readInt() == SSL_REQUEST_CODE) {
            LOGGER.trace("Received SSL negotiation pkg");
            buffer.markReaderIndex();
            ackSslRequest(pipeline.channel());
            // add the ssl handler which must come first in the pipeline
            SslHandler sslHandler = sslContext.newHandler(pipeline.channel().alloc());
            pipeline.addFirst(sslHandler);
        } else {
            // ssl message not available, reset the reader offset
            buffer.resetReaderIndex();
        }
        return State.DONE;
    }
}
