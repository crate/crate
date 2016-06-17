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

import com.google.common.base.Function;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.symbol.Symbols;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.crate.protocols.postgres.ConnectionContext.State.STARTUP_HEADER;


/**
 * ConnectionContext for the Postgres wire protocol.<br />
 * This class handles the message flow and dispatching
 *
 *
 * <pre>
 *      Client                              Server
 *
 *  (optional ssl negotiation)
 *
 *
 *          |    SSLRequest                    |
 *          |--------------------------------->|
 *          |                                  |
 *          |     'S' | 'N' | error            |   (always N - ssl not supported)
 *          |<---------------------------------|
 *
 *
 *  startup:
 *
 *          |                                  |
 *          |      StartupMessage              |
 *          |--------------------------------->|
 *          |                                  |
 *          |      AuthenticationOK            |
 *          |<---------------------------------|
 *          |                                  |
 *          |       ParameterStatus            |
 *          |<---------------------------------|
 *          |                                  |
 *          |       ReadyForQuery              |
 *          |<---------------------------------|
 *
 *
 *  simple Query:
 *
 *          +                                  +
 *          |   Q (query)                      |
 *          |--------------------------------->|
 *          |                                  |
 *          |     RowDescription               |
 *          |<---------------------------------|
 *          |                                  |
 *          |     DataRow                      |
 *          |<---------------------------------|
 *          |     DataRow                      |
 *          |<---------------------------------|
 *          |     CommandComplete              |
 *          |<---------------------------------|
 *          |     ReadyForQuery                |
 *          |<---------------------------------|
 * </pre>
 *
 * Take a look at {@link Messages} to see how the messages are structured
 */

class ConnectionContext {

    private final static ESLogger LOGGER = Loggers.getLogger(ConnectionContext.class);

    final MessageDecoder decoder;
    final MessageHandler handler;
    private final SQLOperations sqlOperations;

    private int pkgLength;
    private byte msgType;

    enum State {
        SSL_NEG,
        STARTUP_HEADER,
        STARTUP_BODY,
        MSG_HEADER,
        MSG_BODY
    }

    private State state = STARTUP_HEADER;

    ConnectionContext(SQLOperations sqlOperations) {
        this.sqlOperations = sqlOperations;
        decoder = new MessageDecoder();
        handler = new MessageHandler();
    }

    private static void traceLogProtocol(int protocol) {
        if (LOGGER.isTraceEnabled()) {
            int major = protocol >> 16;
            int minor = protocol & 0x0000FFFF;
            LOGGER.trace("protocol {}.{}", major, minor);
        }
    }

    private static String readCString(ChannelBuffer buffer, Charset charset) {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return new String(bytes, 0, bytes.length - 1, charset);
    }

    private void readStartupMessage(ChannelBuffer buffer) {
        ChannelBuffer channelBuffer = buffer.readBytes(pkgLength);
        while (true) {
            String s = readCString(channelBuffer, StandardCharsets.UTF_8);
            if (s == null) {
                break;
            }
            LOGGER.trace("payload: {}", s);
        }
    }

    private class MessageHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Object m = e.getMessage();
            if (!(m instanceof ChannelBuffer)) {
                ctx.sendUpstream(e);
                return;
            }

            ChannelBuffer buffer = (ChannelBuffer) m;
            final Channel channel = ctx.getChannel();

            switch (state) {
                case SSL_NEG:
                    state = STARTUP_HEADER;
                    buffer.readInt(); // sslCode
                    ChannelBuffer channelBuffer = ChannelBuffers.buffer(1);
                    channelBuffer.writeByte('N');
                    channel.write(channelBuffer).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            LOGGER.trace("sent SSL neg: N");
                        }
                    });
                    return;
                case STARTUP_HEADER:
                case MSG_HEADER:
                    throw new IllegalStateException("Decoder should've processed the headers");

                case STARTUP_BODY:
                    state = State.MSG_HEADER;
                    readStartupMessage(buffer);
                    Messages.sendAuthenticationOK(channel);
                    // TODO: probably need to send more stuff
                    Messages.sendParameterStatus(channel, "server_encoding", "UTF8");
                    Messages.sendParameterStatus(channel, "client_encoding", "UTF8");
                    Messages.sendParameterStatus(channel, "datestyle", "ISO");
                    Messages.sendReadyForQuery(channel);
                    return;
                case MSG_BODY:
                    state = State.MSG_HEADER;
                    LOGGER.trace("msg={} readableBytes={}", msgType, buffer.readableBytes());
                    switch (msgType) {
                        case 'Q': // Query (simple)
                            String query = readCString(buffer, StandardCharsets.UTF_8);
                            handleSimpleQuery(query, channel);
                            return;
                        case 'X': // Terminate
                            channel.close();
                            return;
                        default:
                            Messages.sendErrorResponse(channel, "Unsupported messageType: " + msgType);
                            return;
                    }
            }
            throw new IllegalStateException("Illegal state: " + state);
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            LOGGER.error(e.toString(), e.getCause());
        }
    }

    /**
     * FrameDecoder that makes sure that a full message is in the buffer before delegating work to the MessageHandler
     */
    private class MessageDecoder extends FrameDecoder {

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            switch (state) {
                /**
                 * StartupMessage:
                 * | int32 length | int32 protocol | [ string paramKey | string paramValue , ... ]
                 */
                case STARTUP_HEADER:
                    if (buffer.readableBytes() < 8) {
                        return null;
                    }
                    pkgLength = buffer.readInt() - 8; // exclude length itself and protocol
                    if (pkgLength == 0) {
                        // SSL negotiation pkg
                        LOGGER.trace("Received SSL negotiation pkg");
                        state = State.SSL_NEG;
                        return buffer;
                    }
                    LOGGER.trace("Header pkgLength: {}", pkgLength);
                    int protocol = buffer.readInt();
                    traceLogProtocol(protocol);
                    state = State.STARTUP_BODY;
                    return nullOrBuffer(buffer);
                /**
                 * Regular Data Packet:
                 * | char tag | int32 len | payload
                 */
                case MSG_HEADER:
                    if (buffer.readableBytes() < 5) {
                        return null;
                    }
                    msgType = buffer.readByte();
                    pkgLength = buffer.readInt() - 4; // exclude length itself
                    LOGGER.trace("Received msg={} length={}", ((char) msgType), pkgLength);
                    state = State.MSG_BODY;
                    return nullOrBuffer(buffer);
                case MSG_BODY:
                case STARTUP_BODY:
                    return nullOrBuffer(buffer);
            }
            throw new IllegalStateException("Invalid state " + state);
        }

        /**
         * return null if there aren't enough bytes to read the whole message. Otherwise returns the buffer.
         *
         * If null is returned the decoder will be called again, otherwise the MessageHandler will be called next.
         */
        private ChannelBuffer nullOrBuffer(ChannelBuffer buffer) {
            if (buffer.readableBytes() < pkgLength) {
                return null;
            }
            return buffer;
        }
    }

    private void handleSimpleQuery(final String query, final Channel channel) {
        LOGGER.trace("query={}", query);

        // TODO: hack for unsupported `SET datetype = 'ISO'` that's sent by psycopg2 if a connection is established
        if (query.startsWith("SET")) {
            Messages.sendCommandComplete(channel, "SET");
            Messages.sendReadyForQuery(channel);
            return;
        }
        try {
            sqlOperations.simpleQuery(query, new Function<AnalyzedRelation, RowReceiver>() {
                @Nullable
                @Override
                public RowReceiver apply(@Nullable AnalyzedRelation input) {
                    assert input != null : "relation must not be null";
                    Messages.sendRowDescription(channel, input.fields());
                    return new PsqlWireRowReceiver(query, channel, Symbols.extractTypes(input.fields()));
                }
            });
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, t.getMessage());
        }
    }

}
