/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.protocols.postgres;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * Handler that processes an optional SSLRequest and rejects SSL.
 */
public final class SslReqRejectingHandler implements SslReqHandler {

    private final Logger LOGGER;

    public SslReqRejectingHandler(Settings settings) {
        LOGGER = Loggers.getLogger(SslReqRejectingHandler.class, settings);
        LOGGER.info("SSL support is disabled.");
    }

    @Override
    public State process(ByteBuf buffer, ChannelPipeline pipeline) {
        if (buffer.readableBytes() < SSL_REQUEST_BYTE_LENGTH) {
            return State.WAITING_FOR_INPUT;
        }
        // mark the buffer so we can jump back if we don't handle this startup
        buffer.markReaderIndex();
        // reads the total message length (int) and the SSL request code (int)
        if (buffer.readInt() == SSL_REQUEST_BYTE_LENGTH && buffer.readInt() == SSL_REQUEST_CODE) {
            // optional SSL negotiation pkg
            LOGGER.trace("Received SSL negotiation pkg");
            rejectSslRequest(pipeline.channel());
            buffer.markReaderIndex();
        } else {
            buffer.resetReaderIndex();
        }
        return State.DONE;
    }

}
