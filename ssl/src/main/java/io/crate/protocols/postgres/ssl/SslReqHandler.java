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

package io.crate.protocols.postgres.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

/**
 * The handler interface for dealing with Postgres SSLRequest messages.
 *
 * https://www.postgresql.org/docs/current/static/protocol-flow.html#AEN113116
 */
public interface SslReqHandler {

    Logger LOGGER = Loggers.getLogger(SslReqHandler.class);

    /** Bytes to be available to the handler to process the message */
    int NUM_BYTES_REQUIRED = 8;
    /* The payload of the SSL Request message */
    int SSL_REQUEST_CODE = 80877103;

    enum State {
        WAITING_FOR_INPUT,
        DONE
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
     * @param pipeline The Netty pipeline which may be modified
     * @param buffer The buffer with incoming data
     * @return The state of the handler
     */
    State process(ChannelPipeline pipeline, ByteBuf buffer);

}
