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

import io.crate.action.sql.SQLOperations;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.Exceptions;
import io.crate.types.DataType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
 * Simple Query:
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
 *
 * Extended Query
 *
 *          +                                  +
 *          |  Parse                           |
 *          |--------------------------------->|
 *          |                                  |
 *          |  ParseComplete or ErrorResponse  |
 *          |<---------------------------------|
 *          |                                  |
 *          |  Bind                            |
 *          |--------------------------------->|
 *          |                                  |
 *          |  BindComplete or ErrorResponse   |
 *          |<---------------------------------|
 *          |                                  |
 *          |  Describe (optional)             |
 *          |--------------------------------->|
 *          |                                  |
 *          |  RowDescription (optional)       |
 *          |<-------------------------------- |
 *          |                                  |
 *          |  Execute                         |
 *          |--------------------------------->|
 *          |                                  |
 *          |  DataRow |                       |
 *          |  CommandComplete |               |
 *          |  EmptyQueryResponse |            |
 *          |  ErrorResponse                   |
 *          |<---------------------------------|
 *          |                                  |
 *          |  Sync                            |
 *          |--------------------------------->|
 *          |                                  |
 *          |  ReadyForQuery                   |
 *          |<---------------------------------|
 * </pre>
 *
 * Take a look at {@link Messages} to see how the messages are structured.
 *
 * See https://www.postgresql.org/docs/current/static/protocol-flow.html for a more detailed description of the message flow
 */

class ConnectionContext {

    private final static ESLogger LOGGER = Loggers.getLogger(ConnectionContext.class);

    final MessageDecoder decoder;
    final MessageHandler handler;
    private final SQLOperations sqlOperations;

    private int msgLength;
    private byte msgType;
    private SQLOperations.Session session;

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

    private static String readCString(ChannelBuffer buffer) {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
    }

    private SQLOperations.Session readStartupMessage(ChannelBuffer buffer) {
        ChannelBuffer channelBuffer = buffer.readBytes(msgLength);
        String defaultSchema = null;
        while (true) {
            String key = readCString(channelBuffer);
            if (key == null) {
                break;
            }
            String value = readCString(channelBuffer);
            LOGGER.trace("payload: key={} value={}", key, value);
            if (key.equals("database") && !"".equals(value)) {
                defaultSchema = value;
            }
        }
        return sqlOperations.createSession(defaultSchema);
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
                    session = readStartupMessage(buffer);
                    Messages.sendAuthenticationOK(channel);
                    // TODO: probably need to send more stuff
                    Messages.sendParameterStatus(channel, "server_encoding", "UTF8");
                    Messages.sendParameterStatus(channel, "client_encoding", "UTF8");
                    Messages.sendParameterStatus(channel, "datestyle", "ISO");
                    Messages.sendReadyForQuery(channel);
                    return;
                case MSG_BODY:
                    state = State.MSG_HEADER;
                    LOGGER.trace("msg={} msgLength={} readableBytes={}", ((char) msgType), msgLength, buffer.readableBytes());
                    switch (msgType) {
                        case 'Q': // Query (simple)
                            handleSimpleQuery(buffer, channel);
                            return;
                        case 'P':
                            handleParseMessage(buffer, channel);
                            return;
                        case 'B':
                            handleBindMessage(buffer, channel);
                            return;
                        case 'D':
                            handleDescribeMessage(buffer, channel);
                            return;
                        case 'E':
                            handleExecute(buffer, channel);
                            return;
                        case 'S':
                            handleSync(channel);
                            return;
                        case 'X': // Terminate
                            channel.close();
                            session = null;
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
     * Parse Message
     * header:
     * | 'P' | int32 len
     *
     * body:
     * | string statementName | string query | int16 numParamTypes |
     *      foreach param:
     *      | int32 type_oid (zero = unspecified)
     */
    private void handleParseMessage(ChannelBuffer buffer, final Channel channel) {
        String statementName = readCString(buffer);
        final String query = readCString(buffer);
        short numParams = buffer.readShort();
        List<DataType> paramTypes = new ArrayList<>(numParams);
        for (int i = 0; i < numParams; i++) {
            paramTypes.add(PGTypes.fromOID(buffer.readInt()));
        }
        try {
            session.parse(statementName, query, paramTypes);
            Messages.sendParseComplete(channel);
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, Exceptions.messageOf(t));
        }
    }

    /**
     * Bind Message
     * Header:
     * | 'B' | int32 len
     *
     * Body:
     * | string portalName | string statementName
     * | int16 numFormatCodes
     *      foreach
     *      | int16 formatCode
     * | int16 numParams
     *      foreach
     *      | int32 valueLength
     *      | byteN value
     * | int16 numResultColumnFormatCodes
     *      foreach
     *      | int16 formatCode
     */
    private void handleBindMessage(ChannelBuffer buffer, Channel channel) {
        String portalName = readCString(buffer);
        String statementName = readCString(buffer);

        short numFormatCodes = buffer.readShort();
        for (int i = 0; i < numFormatCodes; i++) {
            short formatNode = buffer.readShort();
        }

        short numParams = buffer.readShort();
        List<Object> params = Collections.emptyList();
        if (numParams > 0) {
            params = new ArrayList<>(numParams);
            for (int i = 0; i < numParams; i++) {
                int valueLength = buffer.readInt();
                if (valueLength == -1) {
                    params.add(null);
                } else {
                    DataType paramType = session.getParamType(i);
                    PGType pgType = PGTypes.CRATE_TO_PG_TYPES.get(paramType);
                    params.add(pgType.readValue(buffer, valueLength));
                }
            }
        }
        short numResultFormatCodes = buffer.readShort();
        for (int i = 0; i < numResultFormatCodes; i++) {
            buffer.readShort();
        }

        try {
            session.bind(portalName, statementName, params);
            Messages.sendBindComplete(channel);
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, Exceptions.messageOf(t));
        }
    }

    /**
     * Describe Message
     * Header:
     *  | 'D' | int32 len
     *
     * Body:
     *  | 'S' = prepared statement or 'P' = portal
     *  | string nameOfPortalOrStatement
     */
    private void handleDescribeMessage(ChannelBuffer buffer, Channel channel) {
        byte type = buffer.readByte();
        String portalOrStatement = readCString(buffer);
        try {
            Collection<Field> fields = session.describe((char) type, portalOrStatement);
            Messages.sendRowDescription(channel, fields);
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, Exceptions.messageOf(t));
        }
    }

    /**
     * Execute Message
     * Header:
     *  | 'E' | int32 len
     *
     * Body:
     *  | string portalName
     *  | int32 maxRows (0 = unlimited)
     */
    private void handleExecute(ChannelBuffer buffer, Channel channel) {
        String portalName = readCString(buffer);
        int maxRows = buffer.readInt();
        try {
            session.execute(portalName, maxRows,
                new PsqlWireRowReceiver(session.query(), channel, session.outputTypes()));
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, Exceptions.messageOf(t));
        }
    }

    private void handleSync(Channel channel) {
        try {
            session.sync();
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, Exceptions.messageOf(t));
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
                    msgLength = buffer.readInt() - 8; // exclude length itself and protocol
                    if (msgLength == 0) {
                        // SSL negotiation pkg
                        LOGGER.trace("Received SSL negotiation pkg");
                        state = State.SSL_NEG;
                        return buffer;
                    }
                    LOGGER.trace("Header pkgLength: {}", msgLength);
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
                    msgLength = buffer.readInt() - 4; // exclude length itself
                    LOGGER.trace("Received msg={} length={}", ((char) msgType), msgLength);
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
            if (buffer.readableBytes() < msgLength) {
                return null;
            }
            return buffer;
        }
    }

    private void handleSimpleQuery(ChannelBuffer buffer, final Channel channel) {
        String query = readCString(buffer);
        assert query != null : "query must not be nulL";
        LOGGER.trace("query={}", query);

        // TODO: hack for unsupported `SET datetype = 'ISO'` that's sent by psycopg2 if a connection is established
        if (query.startsWith("SET")) {
            Messages.sendCommandComplete(channel, "SET");
            Messages.sendReadyForQuery(channel);
            return;
        }
        try {
            session.parse("", query, Collections.<DataType>emptyList());
            session.bind("", "", Collections.emptyList());
            List<Field> fields = session.describe('S', "");
            Messages.sendRowDescription(channel, fields);
            session.execute("", 0, new PsqlWireRowReceiver(query, channel, Symbols.extractTypes(fields)));
            session.sync();
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, Exceptions.messageOf(t));
        }
    }

}
