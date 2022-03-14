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

import io.crate.action.sql.DescribeResult;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.action.sql.SessionContext;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.auth.AccessControl;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.user.User;
import io.crate.user.UserManager;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.planner.DependencyCarrier;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.user.StubUserManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.ArgumentMatchers.anyChar;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresWireProtocolTest extends CrateDummyClusterServiceUnitTest {

    private static final Provider<UserManager> USER_MANAGER_PROVIDER = StubUserManager::new;

    private SQLOperations sqlOperations;
    private List<Session> sessions = new ArrayList<>();
    private EmbeddedChannel channel;

    @Before
    public void prepare() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text not null)")
            .build();
        sqlOperations = new SQLOperations(
            e.nodeCtx,
            e.analyzer,
            e.planner,
            () -> mock(DependencyCarrier.class),
            new JobsLogs(() -> true),
            Settings.EMPTY,
            clusterService,
            USER_MANAGER_PROVIDER
        ) {
            @Override
            public Session createSession(@Nullable String defaultSchema, @Nullable User user) {
                Session session = super.createSession(defaultSchema, user);
                sessions.add(session);
                return session;
            }
        };
    }

    @After
    public void dispose() throws Exception {
        if (channel != null) {
            channel.finishAndReleaseAll();
            channel.close().awaitUninterruptibly().get(5, TimeUnit.SECONDS);
            channel = null;
        }
    }

    @Test
    public void testHandleEmptySimpleQuery() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> null),
                null, null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        try {
            Messages.writeCString(buffer, ";".getBytes(StandardCharsets.UTF_8));
            ctx.handleSimpleQuery(buffer, new DelayableWriteChannel(channel));
        } finally {
            buffer.release();
        }

        ByteBuf firstResponse = channel.readOutbound();
        byte[] responseBytes = new byte[5];
        try {
            firstResponse.readBytes(responseBytes);
            // EmptyQueryResponse: 'I' | int32 len
            assertThat(responseBytes, is(new byte[]{'I', 0, 0, 0, 4}));
        } finally {
            firstResponse.release();
        }

        ByteBuf secondResponse = channel.readOutbound();
        try {
            responseBytes = new byte[6];
            secondResponse.readBytes(responseBytes);
            // ReadyForQuery: 'Z' | int32 len | 'I'
            assertThat(responseBytes, is(new byte[]{'Z', 0, 0, 0, 5, 'I'}));
        } finally {
            secondResponse.release();
        }
    }

    @Test
    public void test_channel_is_flushed_after_receiving_flush_request() throws Exception {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        Session session = mock(Session.class);
        when(sqlOperations.createSession(any(String.class), any(User.class))).thenReturn(session);
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);
        AtomicBoolean flushed = new AtomicBoolean(false);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler) {
            @Override
            public Channel flush() {
                flushed.set(true);
                return super.flush();
            }
        };

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "", "select ?", new int[0]);
        ClientMessages.sendFlush(buffer);

        channel.writeInbound(buffer);
        channel.releaseInbound();

        assertThat(flushed.get(), is(true));
    }

    @Test
    public void testBindMessageCanBeReadIfTypeForParamsIsUnknown() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "S1", "select ?, ?", new int[0]); // no type hints for parameters

        List<Object> params = Arrays.asList(10, 20);
        ClientMessages.sendBindMessage(buffer, "P1", "S1", params);

        channel.writeInbound(buffer);
        channel.releaseInbound();

        Session session = sessions.get(0);
        // If the query can be retrieved via portalName it means bind worked
        assertThat(session.getQuery("P1"), is("select ?, ?"));
    }

    @Test
    public void testDescribePortalMessage() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc");
            ClientMessages.sendParseMessage(buffer,
                "S1",
                "select ? in (1, 2, 3)",
                new int[] { PGTypes.get(DataTypes.INTEGER).oid() });
                ClientMessages.sendBindMessage(buffer,
                    "P1",
                    "S1",
                    Collections.singletonList(1));
            channel.writeInbound(buffer);
            channel.releaseInbound();

            // we're not interested in the startup, parse, or bind replies
            channel.flushOutbound();
            channel.releaseOutbound();
            channel.outboundMessages().clear();
        }
        {
            // try portal describe message
            ByteBuf buffer = Unpooled.buffer();
            ClientMessages.sendDescribeMessage(buffer, ClientMessages.DescribeType.PORTAL, "P1");
            channel.writeInbound(buffer);
            channel.releaseInbound();

            // we should get back a RowDescription message
            channel.flushOutbound();
            ByteBuf response = channel.readOutbound();
            try {
                assertThat(response.readByte(), is((byte) 'T'));
                assertThat(response.readInt(), is(46));
                assertThat(response.readShort(), is((short) 1));
                assertThat(PostgresWireProtocol.readCString(response), is("($1 = ANY([1, 2, 3]))"));

                assertThat(response.readInt(), is(0));
                assertThat(response.readShort(), is((short) 0));
                assertThat(response.readInt(), is(PGTypes.get(DataTypes.BOOLEAN).oid()));
                assertThat(response.readShort(), is(PGTypes.get(DataTypes.BOOLEAN).typeLen()));
                assertThat(response.readInt(), is(PGTypes.get(DataTypes.LONG).typeMod()));
                assertThat(response.readShort(), is((short) 0));
            } finally {
                response.release();
            }
        }
    }

    @Test
    public void testDescribeStatementMessage() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc");
            ClientMessages.sendParseMessage(buffer, "S1", "select ? in (1, 2, 3)", new int[0]);
            channel.writeInbound(buffer);
            channel.releaseInbound();

            // we're not interested in the startup, parse, or bind replies
            channel.flushOutbound();
            channel.releaseOutbound();
            channel.outboundMessages().clear();
        }
        {
            // try the describe statement variant
            ByteBuf buffer = Unpooled.buffer();
            ClientMessages.sendDescribeMessage(buffer, ClientMessages.DescribeType.STATEMENT, "S1");
            channel.writeInbound(buffer);
            channel.releaseInbound();

            // we should get back a ParameterDescription message
            channel.flushOutbound();
            ByteBuf response = channel.readOutbound();
            try {
                assertThat(response.readByte(), is((byte) 't'));
                assertThat(response.readInt(), is(10));
                assertThat(response.readShort(), is((short) 1));
                assertThat(response.readInt(), is(PGTypes.get(DataTypes.INTEGER).oid()));
            } finally {
                response.release();
            }

            // we should get back a RowDescription message
            response = channel.readOutbound();
            try {
                assertThat(response.readByte(), is((byte) 'T'));
                assertThat(response.readInt(), is(46));
                assertThat(response.readShort(), is((short) 1));
                assertThat(PostgresWireProtocol.readCString(response), is("($1 = ANY([1, 2, 3]))"));

                assertThat(response.readInt(), is(0));
                assertThat(response.readShort(), is((short) 0));
                assertThat(response.readInt(), is(PGTypes.get(DataTypes.BOOLEAN).oid()));
                assertThat(response.readShort(), is(PGTypes.get(DataTypes.BOOLEAN).typeLen()));
                assertThat(response.readInt(), is(PGTypes.get(DataTypes.LONG).typeMod()));
                assertThat(response.readShort(), is((short) 0));
            } finally {
                response.release();
            }
        }
    }

    @Test
    public void test_row_description_for_statement_on_single_table_includes_table_oid() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc");
            ClientMessages.sendParseMessage(buffer, "S1", "SELECT name FROM users", new int[0]);
            channel.writeInbound(buffer);
            channel.releaseInbound();

            // we're not interested in the startup, parse, or bind replies
            channel.flushOutbound();
            channel.releaseOutbound();
            channel.outboundMessages().clear();
        }
        {
            ByteBuf buffer = Unpooled.buffer();
            ClientMessages.sendDescribeMessage(buffer, ClientMessages.DescribeType.STATEMENT, "S1");
            channel.writeInbound(buffer);
            channel.releaseInbound();

            // we should get back a ParameterDescription message, but not interesting for this test case
            channel.flushOutbound();
            ByteBuf response = channel.readOutbound();
            response.release();

            // we should get back a RowDescription message
            response = channel.readOutbound();
            try {
                assertThat(response.readByte(), is((byte) 'T'));
                assertThat(response.readInt(), is(29));
                assertThat(response.readShort(), is((short) 1));
                assertThat(PostgresWireProtocol.readCString(response), is("name"));

                assertThat("table_oid", response.readInt(), is(893280107));
                assertThat("attr_num", response.readShort(), is((short) 1));
                var pgType = PGTypes.get(DataTypes.STRING);
                assertThat(response.readInt(), is(pgType.oid()));
                assertThat(response.readShort(), is(pgType.typeLen()));
                assertThat(response.readInt(), is(pgType.typeMod()));
                assertThat("format_code", response.readShort(), is((short) 0));
            } finally {
                response.release();
            }
        }

    }

    @Test
    public void testSslRejection() {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> null),
                null, null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendSslReqMessage(buffer);
        channel.writeInbound(buffer);

        // We should get back an 'N'...
        ByteBuf responseBuffer = channel.readOutbound();
        try {
            byte response = responseBuffer.readByte();
            assertEquals(response, 'N');
        } finally {
            responseBuffer.release();
        }

        // ...and continue unencrypted (no ssl handler)
        for (Map.Entry<String, ChannelHandler> entry : channel.pipeline()) {
            assertThat(entry.getValue(), isOneOf(ctx.decoder, ctx.handler));
        }
    }

    @Test
    public void testCrateServerVersionIsReceivedOnStartup() throws Exception {
        PostgresWireProtocol ctx = new PostgresWireProtocol(
            sqlOperations, sessionContext -> AccessControl.DISABLED, new AlwaysOKAuthentication(userName -> User.CRATE_USER),
            null,
            null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buf = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buf, "doc");
        channel.writeInbound(buf);
        channel.releaseInbound();

        ByteBuf respBuf;
        respBuf = channel.readOutbound();
        try {
            assertThat((char) respBuf.readByte(), is('R')); // Auth OK
        } finally {
            respBuf.release();
        }

        respBuf = channel.readOutbound();
        try {
            assertThat((char) respBuf.readByte(), is('S')); // ParameterStatus
            respBuf.readInt(); // length
            String key = PostgresWireProtocol.readCString(respBuf);
            String value = PostgresWireProtocol.readCString(respBuf);

            assertThat(key, is("crate_version"));
            assertThat(value, is(Version.CURRENT.externalNumber()));
        } finally {
            respBuf.release();
        }
    }

    @Test
    public void testPasswordMessageAuthenticationProcess() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                sessionContext -> AccessControl.DISABLED,
                new Authentication() {

                    @Override
                    public AuthenticationMethod resolveAuthenticationType(String user, ConnectionProperties connectionProperties) {
                        return new AuthenticationMethod() {
                            @Nullable
                            @Override
                            public User authenticate(String userName, @Nullable SecureString passwd, ConnectionProperties connProperties) {
                                return null;
                            }

                            @Override
                            public String name() {
                                return "password";
                            }
                        };
                    }
                },
                null, null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf respBuf;
        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        channel.writeInbound(buffer);

        respBuf = channel.readOutbound();
        try {
            assertThat((char) respBuf.readByte(), is('R')); // AuthenticationCleartextPassword
        } finally {
            respBuf.release();
        }

        buffer = Unpooled.buffer();
        ClientMessages.sendPasswordMessage(buffer, "pw");
        channel.writeInbound(buffer);

        respBuf = channel.readOutbound();
        try {
            assertThat((char) respBuf.readByte(), is('R')); // Auth OK
        } finally {
            respBuf.release();
        }
    }

    @Test
    public void testSessionCloseOnTerminationMessage() throws Exception {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        Session session = mock(Session.class);
        when(sqlOperations.createSession(any(String.class), any(User.class))).thenReturn(session);
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionContext -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendTermination(buffer);
        channel.writeInbound(buffer);
        channel.releaseInbound();

        verify(session, times(1)).close();
    }

    @Test
    public void testHandleMultipleSimpleQueries() {
        submitQueriesThroughSimpleQueryMode("select 'first'; select 'second';", null);
        readReadyForQueryMessage(channel);
        assertThat(channel.outboundMessages().size() , is(0));
    }

    @Test
    public void testHandleMultipleSimpleQueriesWithQueryFailure() {
        submitQueriesThroughSimpleQueryMode("select 'first'; select 'second';", new RuntimeException("fail"));
        readErrorResponse(channel, (byte) 110);
        readReadyForQueryMessage(channel);
        assertThat(channel.outboundMessages().size() , is(0));
    }

    @Test
    public void testKillExceptionSendsReadyForQuery() {
        submitQueriesThroughSimpleQueryMode("select 1;", JobKilledException.of("with fire"));
        readErrorResponse(channel, (byte) 75);
        readReadyForQueryMessage(channel);
    }

    private void submitQueriesThroughSimpleQueryMode(String statements, @Nullable Throwable failure) {
        SQLOperations sqlOperations = Mockito.mock(SQLOperations.class);
        Session session = mock(Session.class);
        SessionContext sessionContext = new SessionContext(User.CRATE_USER);
        when(session.sessionContext()).thenReturn(sessionContext);
        when(sqlOperations.createSession(any(String.class), any(User.class))).thenReturn(session);
        DescribeResult describeResult = mock(DescribeResult.class);
        when(describeResult.getFields()).thenReturn(null);
        when(session.describe(anyChar(), anyString())).thenReturn(describeResult);
        when(session.transactionState()).thenReturn(TransactionState.IDLE);

        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                sessionCtx -> AccessControl.DISABLED,
                new AlwaysOKAuthentication(userName -> User.CRATE_USER),
                null, null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        if (failure != null) {
            when(session.sync()).thenAnswer(invocationOnMock -> {
                Messages.sendErrorResponse(channel, AccessControl.DISABLED, failure);
                return CompletableFuture.failedFuture(failure);
            });
        } else {
            when(session.sync()).thenReturn(CompletableFuture.completedFuture(null));
        }

        sendStartupMessage(channel);
        readAuthenticationOK(channel);
        skipParameterMessages(channel);
        readReadyForQueryMessage(channel);

        ByteBuf query = Unpooled.buffer();
        try {
            // the actual statements don't have to be valid as they are not executed
            Messages.writeCString(query, statements.getBytes(StandardCharsets.UTF_8));
            ctx.handleSimpleQuery(query, new DelayableWriteChannel(channel));
        } finally {
            query.release();
        }
    }

    private static void sendStartupMessage(EmbeddedChannel channel) {
        ByteBuf startupMsg = Unpooled.buffer();
        ClientMessages.sendStartupMessage(startupMsg, "db");
        channel.writeInbound(startupMsg);
        channel.releaseInbound();
    }

    private static void readAuthenticationOK(EmbeddedChannel channel) {
        ByteBuf response = channel.readOutbound();
        byte[] responseBytes = new byte[9];
        response.readBytes(responseBytes);
        response.release();
        // AuthenticationOK: 'R' | int32 len | int32 code
        assertThat(responseBytes, is(new byte[]{'R', 0, 0, 0, 8, 0, 0, 0, 0}));
    }

    private static void skipParameterMessages(EmbeddedChannel channel) {
        int messagesToSkip = 0;
        for (Object msg : channel.outboundMessages()) {
            ByteBuf buf = (ByteBuf) msg;
            byte messageType = buf.getByte(0);
            if (messageType != 'S') {
                break;
            }
            messagesToSkip++;
        }
        for (int i = 0; i < messagesToSkip; i++) {
            ByteBuf resp = channel.readOutbound();
            resp.release();
        }
    }

    private static void readReadyForQueryMessage(EmbeddedChannel channel) {
        ByteBuf response = channel.readOutbound();
        byte[] responseBytes = new byte[6];
        response.readBytes(responseBytes);
        response.release();
        // ReadyForQuery: 'Z' | int32 len | 'I'
        assertThat(responseBytes, is(new byte[]{'Z', 0, 0, 0, 5, 'I'}));
    }

    private static void readErrorResponse(EmbeddedChannel channel, byte len) {
        ByteBuf response = channel.readOutbound();
        byte[] responseBytes = new byte[5];
        response.readBytes(responseBytes);
        response.release();
        // ErrorResponse: 'E' | int32 len | ...
        assertThat(responseBytes, is(new byte[]{'E', 0, 0, 0, len}));
    }
}
