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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import io.crate.action.sql.DescribeResult;
import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.auth.AccessControl;
import io.crate.auth.AlwaysOKAuthentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;

public class PostgresWireProtocolTest extends CrateDummyClusterServiceUnitTest {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private Sessions sqlOperations;
    private EmbeddedChannel channel;
    private SQLExecutor executor;

    @BeforeClass
    public static void forceEnglishLocale() {
        // BouncyCastle is parsing date objects with the system locale while creating self-signed SSL certs
        // This fails for certain locales, e.g. 'ks'.
        // Until this is fixed, we force the english locale.
        // See also https://github.com/bcgit/bc-java/issues/405 (different topic, but same root cause)
        Locale.setDefault(Locale.ENGLISH);
    }

    @Before
    public void prepare() throws Exception {
        executor = SQLExecutor.builder(clusterService)
            .addTable("create table users (name text not null)")
            .build();
        sqlOperations = executor.sqlOperations;
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
                mock(Sessions.class),
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of()),
                null
            );
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
        Sessions sqlOperations = mock(Sessions.class);
        Session session = mock(Session.class);
        when(sqlOperations.newSession(any(String.class), any(Role.class))).thenReturn(session);
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null);
        AtomicBoolean flushed = new AtomicBoolean(false);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler) {
            @Override
            public Channel flush() {
                flushed.set(true);
                return super.flush();
            }
        };

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
        ClientMessages.sendParseMessage(buffer, "", "select ?", new int[0]);
        ClientMessages.sendFlush(buffer);

        channel.writeInbound(buffer);
        channel.releaseInbound();

        assertThat(flushed.get(), is(true));
    }

    @Test
    public void testBindMessageCanBeReadIfTypeForParamsIsUnknown() throws Exception {
        var mockedSqlOperations = mock(Sessions.class);
        AtomicReference<Session> sessionRef = new AtomicReference<>();
        when(mockedSqlOperations.newSession(Mockito.anyString(), Mockito.any())).thenAnswer(new Answer<Session>() {

            @Override
            public Session answer(InvocationOnMock invocation) throws Throwable {
                var session = sqlOperations.newSession(
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(1, Role.class)
                );
                sessionRef.set(session);
                return session;
            }
        });
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mockedSqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
        ClientMessages.sendParseMessage(buffer, "S1", "select ?, ?", new int[0]); // no type hints for parameters

        List<Object> params = Arrays.asList(10, 20);
        ClientMessages.sendBindMessage(buffer, "P1", "S1", params);

        channel.writeInbound(buffer);
        channel.releaseInbound();

        Session session = sessionRef.get();
        // If the query can be retrieved via portalName it means bind worked
        assertThat(session.getQuery("P1"), is("select ?, ?"));
    }

    @Test
    public void testDescribePortalMessage() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
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
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
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
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
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
                mock(Sessions.class),
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of()),
                () -> null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.writeSSLReqMessage(buffer);
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
    public void test_ssl_accepted() {
        PostgresWireProtocol ctx = new PostgresWireProtocol(
            mock(Sessions.class),
            new SessionSettingRegistry(Set.of()),
            sessionSettings -> AccessControl.DISABLED,
            chPipeline -> {},
            new AlwaysOKAuthentication(() -> List.of()),
            () -> {
                try {
                    var cert = new SelfSignedCertificate();
                    return SslContextBuilder
                        .forServer(cert.certificate(), cert.privateKey())
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .startTls(false)
                        .build();
                } catch (Exception e) {
                    return null;
                }
            }
        );

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.writeSSLReqMessage(buffer);
        channel.writeInbound(buffer);

        ByteBuf responseBuffer = channel.readOutbound();
        try {
            byte response = responseBuffer.readByte();
            assertEquals(response, 'S');
        } finally {
            responseBuffer.release();
        }

        assertThat(channel.pipeline().first(), instanceOf(SslHandler.class));
    }

    @Test
    public void testCrateServerVersionIsReceivedOnStartup() throws Exception {
        PostgresWireProtocol ctx = new PostgresWireProtocol(
            sqlOperations,
            new SessionSettingRegistry(Set.of()),
            sessionSettings -> AccessControl.DISABLED,
            chPipeline -> {},
            new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
            null
        );
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buf = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buf, "doc", Map.of("user", "crate"));
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
        var sqlOperations = mock(Sessions.class);
        when(sqlOperations.newSession(any(String.class), any(Role.class))).thenReturn(mock(Session.class));
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                (user, connectionProperties) -> new AuthenticationMethod() {
                    @Override
                    public Role authenticate(String userName, @Nullable SecureString passwd, ConnectionProperties connProperties) {
                        return RolesHelper.userOf("dummy");
                    }

                    @Override
                    public String name() {
                        return "password";
                    }
                },
                null
            );
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf respBuf;
        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
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
        Sessions sqlOperations = mock(Sessions.class);
        Session session = mock(Session.class);
        when(sqlOperations.newSession(any(String.class), any(Role.class))).thenReturn(session);
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null
            );
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
        ClientMessages.sendTermination(buffer);
        channel.writeInbound(buffer);
        channel.releaseInbound();

        verify(session, times(1)).close();
    }

    @Test
    public void testHandleSimpleQueryFailing() {
        CompletableFuture<?> completableFuture = new CompletableFuture<>();
        submitQueriesThroughSimpleQueryMode("SELECT 'fail'",
                                            new RuntimeException("fail"),
                                            completableFuture);
        // the completableFuture is not completed but the channel is flushed
        readErrorResponse(channel);
        readReadyForQueryMessage(channel);
        assertThat(channel.outboundMessages().size(), is(0));
    }

    @Test
    public void testHandleMultipleSimpleQueries() {
        submitQueriesThroughSimpleQueryMode("select 'first'; select 'second';");
        readReadyForQueryMessage(channel);
        assertThat(channel.outboundMessages().size(), is(0));
    }

    @Test
    public void testHandleMultipleSimpleQueriesWithQueryFailure() {
        submitQueriesThroughSimpleQueryMode("select 'first'; select 'second';", new RuntimeException("fail"));
        readErrorResponse(channel);
        readReadyForQueryMessage(channel);
        assertThat(channel.outboundMessages().size() , is(0));
    }

    @Test
    public void testKillExceptionSendsReadyForQuery() {
        submitQueriesThroughSimpleQueryMode("select 1;", JobKilledException.of("with fire"));
        readErrorResponse(channel);
        readReadyForQueryMessage(channel);
    }

    @Test
    public void testKeyDataSentDuringStartUp() {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionSettings -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null
            );
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        sendStartupMessage(channel);
        readAuthenticationOK(channel);
        skipParameterMessages(channel);
        KeyData readKeyData = readKeyData(channel);
        assertThat(readKeyData).isNotNull();
        assertThat(ctx.session.id()).isEqualTo(readKeyData.pid());
        assertThat(ctx.session.secret()).isEqualTo(readKeyData.secretKey());
    }

    @Test
    public void testHandleCancelRequestBody() {
        PostgresWireProtocol pg1 =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                context -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null
            );
        PostgresWireProtocol pg2 =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                context -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null
            );

        channel = new EmbeddedChannel(pg1.decoder, pg1.handler);
        sendStartupMessage(channel);
        readAuthenticationOK(channel);
        skipParameterMessages(channel);
        readKeyData(channel);
        readReadyForQueryMessage(channel);
        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendParseMessage(buffer, "", "select 1", new int[0]);
        ClientMessages.sendBindMessage(buffer, "", "", List.of());
        ClientMessages.sendExecute(buffer, "", 0);
        channel.writeInbound(buffer);
        channel.releaseInbound();
        channel.flushOutbound();
        channel.releaseOutbound();
        assertThat(pg1.session).isNotNull();
        assertThat(pg1.session.getMostRecentJobID()).isNotNull();

        channel = new EmbeddedChannel(pg2.decoder, pg2.handler);

        ArgumentCaptor<KillJobsNodeRequest> captureReq = ArgumentCaptor.forClass(KillJobsNodeRequest.class);
        Client clientMock = mock(Client.class);
        when(clientMock.execute(Mockito.any(), captureReq.capture())).thenReturn(new CompletableFuture<>());
        when(executor.dependencyMock.client()).thenReturn(clientMock);

        sendCancelRequest(channel, new KeyData(pg1.session.id(), pg1.session.secret()));

        KillJobsNodeRequest request = captureReq.getValue();
        assertThat(request.innerRequest().toKill()).containsExactly(pg1.session.getMostRecentJobID());

        assertThat(channel.isOpen()).isFalse();
    }

    private void submitQueriesThroughSimpleQueryMode(String statements) {
        submitQueriesThroughSimpleQueryMode(statements, null, null);
    }

    private void submitQueriesThroughSimpleQueryMode(String statements, Throwable failure) {
        submitQueriesThroughSimpleQueryMode(statements, failure, null);
    }

    private void submitQueriesThroughSimpleQueryMode(String statements,
                                                     @Nullable Throwable failure,
                                                     @Nullable CompletableFuture future) {
        Sessions sqlOperations = Mockito.mock(Sessions.class);
        Session session = mock(Session.class);
        when(session.execute(any(String.class), any(int.class), any(RowCountReceiver.class))).thenReturn(future);
        var sessionSettings = new CoordinatorSessionSettings(Role.CRATE_USER);
        when(session.sessionSettings()).thenReturn(sessionSettings);
        when(sqlOperations.newSession(any(String.class), any(Role.class))).thenReturn(session);
        DescribeResult describeResult = mock(DescribeResult.class);
        when(describeResult.getFields()).thenReturn(null);
        when(session.describe(Mockito.anyChar(), Mockito.anyString())).thenReturn(describeResult);
        when(session.transactionState()).thenReturn(TransactionState.IDLE);

        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new SessionSettingRegistry(Set.of()),
                sessionCtx -> AccessControl.DISABLED,
                chPipeline -> {},
                new AlwaysOKAuthentication(() -> List.of(Role.CRATE_USER)),
                null
            );
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        if (failure != null) {
            when(session.sync()).thenThrow(failure);
        } else {
            when(session.sync()).thenReturn(CompletableFuture.completedFuture(null));
        }

        sendStartupMessage(channel);
        readAuthenticationOK(channel);
        skipParameterMessages(channel);
        readKeyData(channel);
        readReadyForQueryMessage(channel);

        ByteBuf query = Unpooled.buffer();
        try {
            // the actual statements don't have to be valid as they are not executed
            Messages.writeCString(query, statements.getBytes(StandardCharsets.UTF_8));
            DelayableWriteChannel delayChannel = new DelayableWriteChannel(channel);
            ctx.handleSimpleQuery(query, delayChannel);
            delayChannel.writePendingMessages();
        } finally {
            query.release();
        }
    }

    private static void sendStartupMessage(EmbeddedChannel channel) {
        ByteBuf startupMsg = Unpooled.buffer();
        ClientMessages.sendStartupMessage(startupMsg, "db", Map.of("user", "crate"));
        channel.writeInbound(startupMsg);
        channel.releaseInbound();
    }


    private static void sendCancelRequest(EmbeddedChannel channel, KeyData keyData) {
        ByteBuf cancelRequest = Unpooled.buffer();
        ClientMessages.sendCancelRequest(cancelRequest, keyData);
        channel.writeInbound(cancelRequest);
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

    private static KeyData readKeyData(EmbeddedChannel channel) {
        ByteBuf response = channel.readOutbound();
        // KeyData: 'K' | int32 request code | int32 process id | int32 secret key
        assertThat((char)response.readByte(), is('K'));
        assertThat(response.readInt(), is(12));

        int pid = response.readInt();
        int secretKey = response.readInt();
        response.release();
        return new KeyData(pid, secretKey);
    }

    private static void readReadyForQueryMessage(EmbeddedChannel channel) {
        ByteBuf response = channel.readOutbound();
        byte[] responseBytes = new byte[6];
        response.readBytes(responseBytes);
        response.release();
        // ReadyForQuery: 'Z' | int32 len | 'I'
        assertThat(responseBytes, is(new byte[]{'Z', 0, 0, 0, 5, 'I'}));
    }

    private static ArrayList<String> readErrorResponse(EmbeddedChannel channel) {
        ByteBuf response = channel.readOutbound();
        ArrayList<String> errorFragments = new ArrayList<>();
        // the byte would actually indicate the field type, but we don't care about that here
        while (response.readByte() != 0) {
            String error = PostgresWireProtocol.readCString(response);
            errorFragments.add(error);
        }
        response.release();
        return errorFragments;
    }
}
