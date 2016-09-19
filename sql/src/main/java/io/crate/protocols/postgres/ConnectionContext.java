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

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbols;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionState;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.crate.protocols.postgres.ConnectionContext.State.STARTUP_HEADER;
import static io.crate.protocols.postgres.FormatCodes.getFormatCode;


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
    private boolean ignoreTillSync = false;

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
        return sqlOperations.createSession(defaultSchema, SQLOperations.Option.NONE, 0);
    }

    private static class ReadyForQueryListener implements CompletionListener {
        private final Channel channel;

        private ReadyForQueryListener(Channel channel) {
            this.channel = channel;
        }

        @Override
        public void onSuccess(@Nullable CompletionState result) {
            Messages.sendReadyForQuery(channel);
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
            Messages.sendReadyForQuery(channel);
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
                    session = readStartupMessage(buffer);
                    Messages.sendAuthenticationOK(channel);

                    Messages.sendParameterStatus(channel, "server_version", "95000");
                    Messages.sendParameterStatus(channel, "server_encoding", "UTF8");
                    Messages.sendParameterStatus(channel, "client_encoding", "UTF8");
                    Messages.sendParameterStatus(channel, "datestyle", "ISO");
                    Messages.sendReadyForQuery(channel);
                    return;
                case MSG_BODY:
                    state = State.MSG_HEADER;
                    LOGGER.trace("msg={} msgLength={} readableBytes={}", ((char) msgType), msgLength, buffer.readableBytes());

                    if (ignoreTillSync && msgType != 'S') {
                        buffer.readBytes(msgLength);
                        return;
                    }
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
                        case 'C':
                            handleClose(buffer, channel);
                            return;
                        case 'X': // Terminate
                            channel.close();
                            return;
                        default:
                            Messages.sendErrorResponse(channel, new UnsupportedOperationException("Unsupported messageType: " + msgType));
                            return;
                    }
            }
            throw new IllegalStateException("Illegal state: " + state);
        }

        private void closeSession() {
            if (session != null) {
                session.close();
                session = null;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            ignoreTillSync = true;
            try {
                Messages.sendErrorResponse(ctx.getChannel(), e.getCause());
            } catch (Throwable t) {
                try {
                    LOGGER.error("Error trying to send error to client", t);
                } catch (Throwable ti) {
                    try {
                        ti.printStackTrace(System.err);
                    } catch (Throwable tx) {
                        // prevent infinite exceptionCaught loop
                    }
                }
            }
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            LOGGER.trace("channelDisconnected");
            closeSession();
            super.channelDisconnected(ctx, e);
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
            int oid = buffer.readInt();
            DataType dataType = PGTypes.fromOID(oid);
            if (dataType == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Can't map PGType with oid=%d to Crate type", oid));
            }
            paramTypes.add(dataType);
        }
        session.parse(statementName, query, paramTypes);
        Messages.sendParseComplete(channel);
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

        FormatCodes.FormatCode[] formatCodes = FormatCodes.fromBuffer(buffer);

        short numParams = buffer.readShort();
        List<Object> params = createList(numParams);
        for (int i = 0; i < numParams; i++) {
            int valueLength = buffer.readInt();
            if (valueLength == -1) {
                params.add(null);
            } else {
                DataType paramType = session.getParamType(statementName, i);
                PGType pgType = PGTypes.get(paramType);
                FormatCodes.FormatCode formatCode = getFormatCode(formatCodes, i);
                switch (formatCode) {
                    case TEXT:
                        params.add(pgType.readTextValue(buffer, valueLength));
                        break;

                    case BINARY:
                        params.add(pgType.readBinaryValue(buffer, valueLength));
                        break;

                    default:
                        Messages.sendErrorResponse(channel, new UnsupportedOperationException(
                            String.format(Locale.ENGLISH, "Unsupported format code '%d' for param '%s'",
                                formatCode.ordinal(), paramType.getName())));
                        return;
                }
            }
        }

        FormatCodes.FormatCode[] resultFormatCodes = FormatCodes.fromBuffer(buffer);
        session.bind(portalName, statementName, params, resultFormatCodes);
        Messages.sendBindComplete(channel);
    }

    private <T> List<T> createList(short size) {
        return size == 0 ? Collections.<T>emptyList() : new ArrayList<T>(size);
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
        Collection<Field> fields = session.describe((char) type, portalOrStatement);
        if (fields == null) {
            Messages.sendNoData(channel);
        } else {
            FormatCodes.FormatCode[] formatCodes = session.getResultFormatCodes(portalOrStatement);

            // Force text streaming for ArrayType
            // TODO: implement ArrayType binary streaming and remove this
            int idx = 0;
            for (Field field : fields) {
                if (field.valueType().id() == ArrayType.ID) {
                    formatCodes[idx] = FormatCodes.FormatCode.TEXT;
                }
                idx++;
            }

            Messages.sendRowDescription(channel, fields, formatCodes);
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
        String query = session.getQuery(portalName);
        if (query.isEmpty()) {
             // remove portal so that it doesn't stick around and no attempt to batch it with follow up statement is made
            session.close((byte)'P', portalName);
            Messages.sendEmptyQueryResponse(channel);
            return;
        }
        ResultReceiver resultReceiver = createResultReceiver(
            channel,
            query,
            session.getOutputTypes(portalName),
            session.getResultFormatCodes(portalName));
        session.execute(portalName, maxRows, resultReceiver);
    }

    private static ResultReceiver createResultReceiver(Channel channel,
                                                       String query,
                                                       @Nullable List<? extends DataType> outputTypes,
                                                       @Nullable FormatCodes.FormatCode[] formatCodes) {
        if (outputTypes == null) {
            // this is a DML query:
            return new RowCountReceiver(query, channel);
        } else {
            // query with resultSet
            return new ResultSetReceiver(query, channel, outputTypes, formatCodes);
        }
    }

    private void handleSync(final Channel channel) {
        if (ignoreTillSync) {
            ignoreTillSync = false;
            session.clearState();
            Messages.sendReadyForQuery(channel);
            return;
        }
        try {
            session.sync(new ReadyForQueryListener(channel));
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, t);
            Messages.sendReadyForQuery(channel);
        }
    }

    /**
     * | 'C' | int32 len | byte portalOrStatement | string portalOrStatementName |
     */
    private void handleClose(ChannelBuffer buffer, Channel channel) {
        byte b = buffer.readByte();
        String portalOrStatementName = readCString(buffer);
        session.close(b, portalOrStatementName);
        Messages.sendCloseComplete(channel);
    }

    @VisibleForTesting
    void handleSimpleQuery(ChannelBuffer buffer, final Channel channel) {
        String query = readCString(buffer);
        assert query != null : "query must not be nulL";

        if (query.isEmpty() || ";".equals(query)) {
            Messages.sendEmptyQueryResponse(channel);
            Messages.sendReadyForQuery(channel);
            return;
        }
        // TODO: support multiple statements
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }
        try {
            session.parse("", query, Collections.<DataType>emptyList());
            session.bind("", "", Collections.emptyList(), null);
            List<Field> fields = session.describe('P', "");
            if (fields == null) {
                RowCountReceiver rowCountReceiver = new RowCountReceiver(query, channel);
                session.execute("", 1, rowCountReceiver);
            } else {
                Messages.sendRowDescription(channel, fields, null);
                ResultSetReceiver resultSetReceiver = new ResultSetReceiver(query, channel, Symbols.extractTypes(fields), null);
                session.execute("", 0, resultSetReceiver);
            }
            session.sync(new ReadyForQueryListener(channel));
        } catch (Throwable t) {
            session.clearState();
            Messages.sendErrorResponse(channel, t);
            Messages.sendReadyForQuery(channel);
        }
    }


    /**
     * FrameDecoder that makes sure that a full message is in the buffer before delegating work to the MessageHandler
     */
    private class MessageDecoder extends FrameDecoder {

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            switch (state) {
                /*
                 * StartupMessage:
                 * | int32 length | int32 protocol | [ string paramKey | string paramValue , ... ]
                 */
                case STARTUP_HEADER:
                    if (buffer.readableBytes() < 8) {
                        return null;
                    }
                    buffer.markReaderIndex();
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
                    return nullOrBuffer(buffer, State.STARTUP_BODY);
                /*
                 * Regular Data Packet:
                 * | char tag | int32 len | payload
                 */
                case MSG_HEADER:
                    if (buffer.readableBytes() < 5) {
                        return null;
                    }
                    buffer.markReaderIndex();
                    msgType = buffer.readByte();
                    msgLength = buffer.readInt() - 4; // exclude length itself
                    return nullOrBuffer(buffer, State.MSG_BODY);
                case MSG_BODY:
                case STARTUP_BODY:
                    return nullOrBuffer(buffer, state);
            }
            throw new IllegalStateException("Invalid state " + state);
        }

        /**
         * return null if there aren't enough bytes to read the whole message. Otherwise returns the buffer.
         *
         * If null is returned the decoder will be called again, otherwise the MessageHandler will be called next.
         */
        private ChannelBuffer nullOrBuffer(ChannelBuffer buffer, State nextState) {
            if (buffer.readableBytes() < msgLength) {
                buffer.resetReaderIndex();
                return null;
            }
            state = nextState;
            return buffer;
        }
    }
}
