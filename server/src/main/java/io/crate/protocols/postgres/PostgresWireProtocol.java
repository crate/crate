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

package io.crate.protocols.postgres;

import static io.crate.protocols.SSL.getSession;
import static io.crate.protocols.postgres.FormatCodes.getFormatCode;
import static io.crate.protocols.postgres.Messages.sendReadyForQuery;

import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.jetbrains.annotations.Nullable;

import io.crate.action.sql.DescribeResult;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.auth.AccessControl;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.auth.Protocol;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.protocols.postgres.DelayableWriteChannel.DelayedWrites;
import io.crate.protocols.postgres.parser.PgArrayParser;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import io.crate.user.User;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslContext;
import joptsimple.OptionParser;
import joptsimple.OptionSet;


/**
 * Netty Handler/FrameDecoder for the Postgres wire protocol.<br />
 * This class handles the message flow and dispatching
 * <p>
 * <p>
 * <pre>
 *      Client                              Server
 *
 *  (optional ssl negotiation)
 *
 *
 *          |    SSLRequest                    |
 *          |--------------------------------->|
 *          |                                  |
 *          |     'S' | 'N' | error            |   (supported in Enterprise version)
 *          |<---------------------------------|
 *
 *
 *  startup:
 *  The authentication flow is handled by implementations of {@link AuthenticationMethod}.
 *
 *          |                                  |
 *          |      StartupMessage              |
 *          |--------------------------------->|
 *          |                                  |
 *          |      Authentication<Method>      |
 *          |      or                          |
 *          |      AuthenticationOK            |
 *          |      or                          |
 *          |      ErrorResponse               |
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
 *          |  Describe Statement (optional)   |
 *          |--------------------------------->|
 *          |                                  |
 *          |  ParameterDescription (optional) |
 *          |<-------------------------------- |
 *          |                                  |
 *          |  RowDescription (optional)       |
 *          |<-------------------------------- |
 *          |                                  |
 *          |  Bind                            |
 *          |--------------------------------->|
 *          |                                  |
 *          |  BindComplete or ErrorResponse   |
 *          |<---------------------------------|
 *          |                                  |
 *          |  Describe Portal (optional)      |
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
 * <p>
 * Take a look at {@link Messages} to see how the messages are structured.
 * <p>
 * See https://www.postgresql.org/docs/current/static/protocol-flow.html for a more detailed description of the message flow
 */

public class PostgresWireProtocol {

    private static final Logger LOGGER = LogManager.getLogger(PostgresWireProtocol.class);
    private static final String PASSWORD_AUTH_NAME = "password";

    public static int SERVER_VERSION_NUM = 140000;
    public static String PG_SERVER_VERSION = "14.0";

    final PgDecoder decoder;
    final MessageHandler handler;
    private final Sessions sessions;
    private final SessionSettingRegistry sessionSettingRegistry;
    private final Function<CoordinatorSessionSettings, AccessControl> getAccessControl;
    private final Authentication authService;
    private final Consumer<ChannelPipeline> addTransportHandler;

    private DelayableWriteChannel channel;
    Session session;
    private boolean ignoreTillSync = false;
    private AuthenticationContext authContext;
    private Properties properties;

    PostgresWireProtocol(Sessions sessions,
                         SessionSettingRegistry sessionSettingRegistry,
                         Function<CoordinatorSessionSettings, AccessControl> getAcessControl,
                         Consumer<ChannelPipeline> addTransportHandler,
                         Authentication authService,
                         Supplier<SslContext> getSslContext) {
        this.sessions = sessions;
        this.sessionSettingRegistry = sessionSettingRegistry;
        this.getAccessControl = getAcessControl;
        this.addTransportHandler = addTransportHandler;
        this.authService = authService;
        this.decoder = new PgDecoder(getSslContext);
        this.handler = new MessageHandler();
    }

    @Nullable
    static String readCString(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
    }

    @Nullable
    private static char[] readCharArray(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).array();
    }

    private Properties readStartupMessage(ByteBuf buffer) {
        Properties properties = new Properties();
        while (true) {
            String key = readCString(buffer);
            if (key == null) {
                break;
            }
            String value = readCString(buffer);
            LOGGER.trace("payload: key={} value={}", key, value);
            if (!"".equals(key) && !"".equals(value)) {
                properties.setProperty(key, value);
            }
        }
        return properties;
    }

    private static class ReadyForQueryCallback implements BiConsumer<Object, Throwable> {
        private final Channel channel;
        private final TransactionState transactionState;

        private ReadyForQueryCallback(Channel channel, TransactionState transactionState) {
            this.channel = channel;
            this.transactionState = transactionState;
        }

        @Override
        public void accept(Object result, Throwable t) {
            sendReadyForQuery(channel, transactionState);
        }
    }

    private class MessageHandler extends SimpleChannelInboundHandler<ByteBuf> {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            channel = new DelayableWriteChannel(ctx.channel());
        }

        @Override
        public boolean acceptInboundMessage(Object msg) throws Exception {
            return true;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
            assert channel != null : "Channel must be initialized";
            try {
                dispatchState(buffer, channel);
            } catch (Throwable t) {
                ignoreTillSync = true;
                try {
                    AccessControl accessControl = session == null
                        ? AccessControl.DISABLED
                        : getAccessControl.apply(session.sessionSettings());
                    Messages.sendErrorResponse(channel, accessControl, t);
                } catch (Throwable ti) {
                    LOGGER.error("Error trying to send error to client: {}", t, ti);
                }
            }
        }

        private void dispatchState(ByteBuf buffer, DelayableWriteChannel channel) {
            switch (decoder.state()) {
                case STARTUP_PARAMETERS:
                    handleStartupBody(buffer, channel);
                    decoder.startupDone();
                    return;

                case CANCEL:
                    handleCancelRequestBody(buffer, channel);
                    return;

                case MSG:
                    LOGGER.trace("msg={} msgLength={} readableBytes={}", ((char) decoder.msgType()), decoder.payloadLength(), buffer.readableBytes());

                    if (ignoreTillSync && decoder.msgType() != 'S') {
                        buffer.skipBytes(decoder.payloadLength());
                        return;
                    }
                    dispatchMessage(buffer, channel);
                    return;
                default:
                    throw new IllegalStateException("Illegal state: " + decoder.state());
            }
        }

        private void dispatchMessage(ByteBuf buffer, DelayableWriteChannel channel) {
            switch (decoder.msgType()) {
                case 'Q': // Query (simple)
                    handleSimpleQuery(buffer, channel);
                    return;
                case 'P':
                    handleParseMessage(buffer, channel);
                    return;
                case 'p':
                    handlePassword(buffer, channel);
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
                case 'H':
                    handleFlush(channel);
                    return;
                case 'S':
                    handleSync(channel);
                    return;
                case 'C':
                    handleClose(buffer, channel);
                    return;
                case 'X': // Terminate (called when jdbc connection is closed)
                    closeSession();
                    channel.close();
                    return;
                default:
                    Messages.sendErrorResponse(
                        channel,
                        session == null
                            ? AccessControl.DISABLED
                            : getAccessControl.apply(session.sessionSettings()),
                        new UnsupportedOperationException("Unsupported messageType: " + decoder.msgType()));
            }
        }

        private void closeSession() {
            if (session != null) {
                session.close();
                session = null;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof SocketException && cause.getMessage().equals("Connection reset")) {
                LOGGER.info("Connection reset. Client likely terminated connection");
                closeSession();
            } else {
                LOGGER.error("Uncaught exception: ", cause);
            }
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            LOGGER.trace("channelDisconnected");
            channel = null;
            closeSession();
            super.channelUnregistered(ctx);
        }
    }

    private void handleStartupBody(ByteBuf buffer, Channel channel) {
        properties = readStartupMessage(buffer);
        initAuthentication(channel);
    }

    private void initAuthentication(Channel channel) {
        String userName = properties.getProperty("user");
        InetAddress address = Netty4HttpServerTransport.getRemoteAddress(channel);

        SSLSession sslSession = getSession(channel);
        ConnectionProperties connProperties = new ConnectionProperties(address, Protocol.POSTGRES, sslSession);

        AuthenticationMethod authMethod = authService.resolveAuthenticationType(userName, connProperties);
        if (authMethod == null) {
            String errorMessage = String.format(
                Locale.ENGLISH,
                "No valid auth.host_based entry found for host \"%s\", user \"%s\". Did you enable TLS in your client?",
                address.getHostAddress(), userName
            );
            Messages.sendAuthenticationError(channel, errorMessage);
        } else {
            authContext = new AuthenticationContext(authMethod, connProperties, userName, LOGGER);
            if (PASSWORD_AUTH_NAME.equals(authMethod.name())) {
                Messages.sendAuthenticationCleartextPassword(channel);
                return;
            }
            finishAuthentication(channel);
        }
    }

    private void finishAuthentication(Channel channel) {
        assert authContext != null : "finishAuthentication() requires an authContext instance";
        try {
            User authenticatedUser = authContext.authenticate();
            String database = properties.getProperty("database");
            session = sessions.newSession(database, authenticatedUser);
            String options = properties.getProperty("options");
            if (options != null) {
                applyOptions(options);
            }
            Messages.sendAuthenticationOK(channel)
                .addListener(f -> sendParams(channel, session.sessionSettings()))
                .addListener(f -> Messages.sendKeyData(channel, session.id(), session.secret()))
                .addListener(f -> {
                    sendReadyForQuery(channel, TransactionState.IDLE);
                    if (properties.containsKey("CrateDBTransport")) {
                        switchToTransportProtocol(channel);
                    }
                });
        } catch (Exception e) {
            Messages.sendAuthenticationError(channel, e.getMessage());
        } finally {
            authContext.close();
            authContext = null;
        }
    }

    private void applyOptions(String options) {
        OptionParser parser = new OptionParser();
        var optionC = parser.accepts("c").withRequiredArg().ofType(String.class);
        OptionSet parseResult = parser.parse(options.split(" "));
        List<String> parsedOptions = parseResult.valuesOf(optionC);
        for (String parsedOption : parsedOptions) {
            String[] parts = parsedOption.split("=");
            if (parts.length != 2) {
                continue;
            }
            String key = parts[0].trim();
            String value = parts[1].trim();
            SessionSetting<?> sessionSetting = sessionSettingRegistry.settings().get(key);
            if (sessionSetting == null) {
                continue;
            }
            sessionSetting.apply(session.sessionSettings(), List.of(Literal.of(value)), symbol -> {
                if (symbol instanceof Literal<?> literal) {
                    return literal.value();
                }
                throw new IllegalStateException("Unexpected symbol: " + symbol);
            });
        }
    }

    private void switchToTransportProtocol(Channel channel) {
        var pipeline = channel.pipeline();
        pipeline.remove("frame-decoder");
        pipeline.remove("handler");

        // SSL is already done via PostgreSQL handshake/auth
        addTransportHandler.accept(pipeline);
    }

    private void sendParams(Channel channel, CoordinatorSessionSettings sessionSettings) {
        Messages.sendParameterStatus(channel, "crate_version", Version.CURRENT.externalNumber());
        Messages.sendParameterStatus(channel, "server_version", PG_SERVER_VERSION);
        Messages.sendParameterStatus(channel, "server_encoding", "UTF8");
        Messages.sendParameterStatus(channel, "client_encoding", "UTF8");
        Messages.sendParameterStatus(channel, "datestyle", sessionSettings.dateStyle());
        Messages.sendParameterStatus(channel, "TimeZone", "UTC");
        Messages.sendParameterStatus(channel, "integer_datetimes", "on");
    }

    /**
     * Flush Message
     * | 'H' | int32 len
     * <p>
     * Flush forces the backend to deliver any data pending in it's output buffers.
     */
    private void handleFlush(Channel channel) {
        try {
            // If we have deferred any executions we need to trigger a sync now because the client is expecting data
            // (That we've been holding back, as we don't eager react to `execute` requests. (We do that to optimize batch inserts))
            // The sync will also trigger a flush eventually if there are deferred executions.
            if (session.hasDeferredExecutions()) {
                session.flush();
            } else {
                channel.flush();
            }
        } catch (Throwable t) {
            Messages.sendErrorResponse(channel, getAccessControl.apply(session.sessionSettings()), t);
        }
    }

    /**
     * Parse Message
     * header:
     * | 'P' | int32 len
     * <p>
     * body:
     * | string statementName | string query | int16 numParamTypes |
     * foreach param:
     * | int32 type_oid (zero = unspecified)
     */
    private void handleParseMessage(ByteBuf buffer, final Channel channel) {
        String statementName = readCString(buffer);
        final String query = readCString(buffer);
        short numParams = buffer.readShort();
        List<DataType<?>> paramTypes = new ArrayList<>(numParams);
        for (int i = 0; i < numParams; i++) {
            int oid = buffer.readInt();
            DataType<?> dataType = PGTypes.fromOID(oid);
            if (dataType == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Can't map PGType with oid=%d to Crate type", oid));
            }
            paramTypes.add(dataType);
        }
        session.parse(statementName, query, paramTypes);
        Messages.sendParseComplete(channel);
    }

    private void handlePassword(ByteBuf buffer, final Channel channel) {
        char[] passwd = readCharArray(buffer);
        if (passwd != null) {
            authContext.setSecurePassword(passwd);
        }
        finishAuthentication(channel);
    }

    /**
     * Bind Message
     * Header:
     * | 'B' | int32 len
     * <p>
     * Body:
     * <pre>
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
     * </pre>
     */
    private void handleBindMessage(ByteBuf buffer, Channel channel) {
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
                        Messages.sendErrorResponse(
                            channel,
                            getAccessControl.apply(session.sessionSettings()),
                            new UnsupportedOperationException(String.format(
                                Locale.ENGLISH,
                                "Unsupported format code '%d' for param '%s'",
                                formatCode.ordinal(),
                                paramType.getName())
                            )
                        );
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
     * | 'D' | int32 len
     * <p>
     * Body:
     * | 'S' = prepared statement or 'P' = portal
     * | string nameOfPortalOrStatement
     */
    private void handleDescribeMessage(ByteBuf buffer, Channel channel) {
        byte type = buffer.readByte();
        String portalOrStatement = readCString(buffer);
        DescribeResult describeResult = session.describe((char) type, portalOrStatement);
        Collection<Symbol> fields = describeResult.getFields();
        if (type == 'S') {
            Messages.sendParameterDescription(channel, describeResult.getParameters());
        }
        if (fields == null) {
            Messages.sendNoData(channel);
        } else {
            var resultFormatCodes = type == 'P' ? session.getResultFormatCodes(portalOrStatement) : null;
            Messages.sendRowDescription(channel, fields, resultFormatCodes, describeResult.relation());
        }
    }

    /**
     * Execute Message
     * Header:
     * | 'E' | int32 len
     * <p>
     * Body:
     * | string portalName
     * | int32 maxRows (0 = unlimited)
     */
    private void handleExecute(ByteBuf buffer, DelayableWriteChannel channel) {
        String portalName = readCString(buffer);
        int maxRows = buffer.readInt();
        String query = session.getQuery(portalName);
        if (query.isEmpty()) {
            // remove portal so that it doesn't stick around and no attempt to batch it with follow up statement is made
            session.close((byte) 'P', portalName);
            Messages.sendEmptyQueryResponse(channel);
            return;
        }
        List<? extends DataType> outputTypes = session.getOutputTypes(portalName);

        // .execute is going async and may execute the query in another thread-pool.
        // The results are later sent to the clients via the `ResultReceiver` created
        // above, The `channel.write` calls - which the `ResultReceiver` makes - may
        // happen in a thread which is *not* a netty thread.
        // If that is the case, netty schedules the writes instead of running them
        // immediately. A consequence of that is that *this* thread can continue
        // processing other messages from the client, and if this thread then sends messages to the
        // client, these are sent immediately, overtaking the result messages of the
        // execute that is triggered here.
        //
        // This would lead to out-of-order messages. For example, we could send a
        // `parseComplete` before the `commandComplete` of the previous statement has
        // been transmitted.
        //
        // To ensure clients receive messages in the correct order we delay all writes
        // The "finish" logic of the ResultReceivers writes out all pending writes/unblocks the channel

        DelayedWrites delayedWrites = channel.delayWrites();
        ResultReceiver<?> resultReceiver;
        if (outputTypes == null) {
            // this is a DML query
            maxRows = 0;
            resultReceiver = new RowCountReceiver(
                query,
                channel,
                delayedWrites,
                getAccessControl.apply(session.sessionSettings())
            );
        } else {
            // query with resultSet
            resultReceiver = new ResultSetReceiver(
                query,
                channel,
                delayedWrites,
                session.transactionState(),
                getAccessControl.apply(session.sessionSettings()),
                Lists2.map(outputTypes, PGTypes::get),
                session.getResultFormatCodes(portalName)
            );
        }
        session.execute(portalName, maxRows, resultReceiver);
    }

    private void handleSync(DelayableWriteChannel channel) {
        if (ignoreTillSync) {
            ignoreTillSync = false;
            // If an error happens all sub-sequent messages can be ignored until the client sends a sync message
            // We need to discard any deferred executions to make sure that the *next* sync isn't executing
            // something we had previously deferred.
            // E.g. JDBC client:
            //  1) `addBatch` -> success (results in bind+execute -> we defer execution)
            //  2) `addBatch` -> failure (ignoreTillSync=true; we stop after bind, no execute, etc..)
            //  3) `sync`     -> sendReadyForQuery (this if branch)
            //  4) p, b, e    -> We've a new query deferred.
            //  5) `sync`     -> We must execute the query from 4, but not 1)
            session.resetDeferredExecutions();
            channel.writePendingMessages();
            sendReadyForQuery(channel, TransactionState.FAILED_TRANSACTION);
            return;
        }
        try {
            ReadyForQueryCallback readyForQueryCallback = new ReadyForQueryCallback(channel, session.transactionState());
            session.sync().whenComplete(readyForQueryCallback);
        } catch (Throwable t) {
            channel.discardDelayedWrites();
            Messages.sendErrorResponse(channel, getAccessControl.apply(session.sessionSettings()), t);
            sendReadyForQuery(channel, TransactionState.FAILED_TRANSACTION);
        }
    }

    /**
     * | 'C' | int32 len | byte portalOrStatement | string portalOrStatementName |
     */
    private void handleClose(ByteBuf buffer, Channel channel) {
        byte b = buffer.readByte();
        String portalOrStatementName = readCString(buffer);
        session.close(b, portalOrStatementName);
        Messages.sendCloseComplete(channel);
    }

    @VisibleForTesting
    void handleSimpleQuery(ByteBuf buffer, final DelayableWriteChannel channel) {
        String queryString = readCString(buffer);
        assert queryString != null : "query must not be nulL";

        if (queryString.isEmpty() || ";".equals(queryString)) {
            Messages.sendEmptyQueryResponse(channel);
            sendReadyForQuery(channel, TransactionState.IDLE);
            return;
        }

        List<Statement> statements;
        try {
            statements = SqlParser.createStatementsForSimpleQuery(
                    queryString,
                    str -> PgArrayParser.parse(
                            str,
                            bytes -> new String(bytes, StandardCharsets.UTF_8)
                    )
                );
        } catch (Exception ex) {
            Messages.sendErrorResponse(channel, getAccessControl.apply(session.sessionSettings()), ex);
            sendReadyForQuery(channel, TransactionState.IDLE);
            return;
        }
        CompletableFuture<?> composedFuture = CompletableFuture.completedFuture(null);
        for (var statement : statements) {
            composedFuture = composedFuture.thenCompose(result -> handleSingleQuery(statement, queryString, channel));
        }
        composedFuture.whenComplete(new ReadyForQueryCallback(channel, TransactionState.IDLE));
    }

    private CompletableFuture<?> handleSingleQuery(Statement statement, String query, DelayableWriteChannel channel) {
        CompletableFuture<?> result = new CompletableFuture<>();

        AccessControl accessControl = getAccessControl.apply(session.sessionSettings());
        try {
            session.analyze("", statement, Collections.emptyList(), query);
            session.bind("", "", Collections.emptyList(), null);
            DescribeResult describeResult = session.describe('P', "");
            List<Symbol> fields = describeResult.getFields();

            if (fields == null) {
                DelayedWrites delayedWrites = channel.delayWrites();
                RowCountReceiver rowCountReceiver = new RowCountReceiver(
                    query,
                    channel,
                    delayedWrites,
                    accessControl
                );
                session.execute("", 0, rowCountReceiver);
            } else {
                Messages.sendRowDescription(channel, fields, null, describeResult.relation());
                DelayedWrites delayedWrites = channel.delayWrites();
                ResultSetReceiver resultSetReceiver = new ResultSetReceiver(
                    query,
                    channel,
                    delayedWrites,
                    TransactionState.IDLE,
                    accessControl,
                    Lists2.map(fields, x -> PGTypes.get(x.valueType())),
                    null
                );
                session.execute("", 0, resultSetReceiver);
            }
            return session.sync();
        } catch (Throwable t) {
            channel.discardDelayedWrites();
            Messages.sendErrorResponse(channel, accessControl, t);
            result.completeExceptionally(t);
            return result;
        }
    }

    private void handleCancelRequestBody(ByteBuf buffer, Channel channel) {
        var keyData = KeyData.of(buffer);

        sessions.cancel(keyData);

        // Cancel request is sent by the client over a new connection.
        // This closes the new connection, not the one running the query.
        handler.closeSession();
        channel.close();
    }
}
