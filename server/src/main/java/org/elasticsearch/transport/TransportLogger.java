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

import io.crate.common.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;

public final class TransportLogger {

    private static final Logger LOGGER = LogManager.getLogger(TransportLogger.class);
    private static final int HEADER_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;

    static void logInboundMessage(TcpChannel channel, InboundMessage message) {
        if (LOGGER.isTraceEnabled()) {
            try {
                String logMessage = format(channel, message, "READ");
                LOGGER.trace(logMessage);
            } catch (IOException e) {
                LOGGER.warn("an exception occurred formatting a READ trace message", e);
            }
        }
    }

    private static String format(TcpChannel channel, BytesReference message, String event) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append(channel);
        int messageLengthWithHeader = HEADER_SIZE + message.length();
        // This is a ping
        if (message.length() == 0) {
            sb.append(" [ping]").append(' ').append(event).append(": ").append(messageLengthWithHeader).append('B');
        } else {
            boolean success = false;
            StreamInput streamInput = message.streamInput();
            try {
                final long requestId = streamInput.readLong();
                final byte status = streamInput.readByte();
                final boolean isRequest = TransportStatus.isRequest(status);
                final String type = isRequest ? "request" : "response";
                final Version version = Version.fromId(streamInput.readInt());
                streamInput.setVersion(version);
                sb.append(" [length: ").append(messageLengthWithHeader);
                sb.append(", request id: ").append(requestId);
                sb.append(", type: ").append(type);
                sb.append(", version: ").append(version);

                if (version.onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                    sb.append(", header size: ").append(streamInput.readInt()).append('B');
                } else {
                    streamInput = decompressingStream(status, streamInput);
                    InboundHandler.assertRemoteVersion(streamInput, version);
                }

                // read and discard headers
                ThreadContext.bwcReadHeaders(streamInput);

                if (isRequest) {
                    if (streamInput.getVersion().onOrAfter(Version.V_4_3_0)) {
                        // discard features
                        streamInput.readStringArray();
                    }
                    sb.append(", action: ").append(streamInput.readString());
                }
                sb.append(']');
                sb.append(' ').append(event).append(": ").append(messageLengthWithHeader).append('B');
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(streamInput);
                } else {
                    IOUtils.closeWhileHandlingException(streamInput);
                }
            }
        }
        return sb.toString();
    }

    private static String format(TcpChannel channel, InboundMessage message, String event) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append(channel);

        if (message.isPing()) {
            sb.append(" [ping]").append(' ').append(event).append(": ").append(6).append('B');
        } else {
            boolean success = false;
            Header header = message.getHeader();
            int networkMessageSize = header.getNetworkMessageSize();
            int messageLengthWithHeader = HEADER_SIZE + networkMessageSize;
            StreamInput streamInput = message.openOrGetStreamInput();
            try {
                final long requestId = header.getRequestId();
                final boolean isRequest = header.isRequest();
                final String type = isRequest ? "request" : "response";
                final String version = header.getVersion().toString();
                sb.append(" [length: ").append(messageLengthWithHeader);
                sb.append(", request id: ").append(requestId);
                sb.append(", type: ").append(type);
                sb.append(", version: ").append(version);

                // TODO: Maybe Fix for BWC
                if (header.needsToReadVariableHeader() == false && isRequest) {
                    sb.append(", action: ").append(header.getActionName());
                }
                sb.append(']');
                sb.append(' ').append(event).append(": ").append(messageLengthWithHeader).append('B');
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(streamInput);
                } else {
                    IOUtils.closeWhileHandlingException(streamInput);
                }
            }
        }
        return sb.toString();
    }

    private static StreamInput decompressingStream(byte status, StreamInput streamInput) throws IOException {
        if (TransportStatus.isCompress(status) && streamInput.available() > 0) {
            try {
                return new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(streamInput));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("stream marked as compressed, but is missing deflate header");
            }
        } else {
            return streamInput;
        }
    }
}
