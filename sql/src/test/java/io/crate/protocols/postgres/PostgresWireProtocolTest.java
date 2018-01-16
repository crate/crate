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

import io.crate.Version;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.auth.AlwaysOKNullAuthentication;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;
import io.crate.planner.DependencyCarrier;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DummyUserManager;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresWireProtocolTest extends CrateDummyClusterServiceUnitTest {

    private static final Provider<UserManager> USER_MANAGER_PROVIDER = DummyUserManager::new;

    private SQLOperations sqlOperations;
    private List<Session> sessions = new ArrayList<>();
    private EmbeddedChannel channel;

    @Before
    public void prepare() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        sqlOperations = new SQLOperations(
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
    public void dispose() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
            channel = null;
        }
    }

    @Test
    public void testHandleEmptySimpleQuery() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                new AlwaysOKNullAuthentication(),
                null);
        EmbeddedChannel channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        try {
            Messages.writeCString(buffer, ";".getBytes(StandardCharsets.UTF_8));
            ctx.handleSimpleQuery(buffer, channel);
        } finally {
            buffer.release();
        }

        ByteBuf firstResponse = channel.readOutbound();
        byte[] responseBytes = new byte[5];
        firstResponse.readBytes(responseBytes);
        // EmptyQueryResponse: 'I' | int32 len
        assertThat(responseBytes, is(new byte[]{'I', 0, 0, 0, 4}));

        ByteBuf secondResponse = channel.readOutbound();
        responseBytes = new byte[6];
        secondResponse.readBytes(responseBytes);
        // ReadyForQuery: 'Z' | int32 len | 'I'
        assertThat(responseBytes, is(new byte[]{'Z', 0, 0, 0, 5, 'I'}));
    }

    @Test
    public void testFlushMessageResultsInSyncCallOnSession() throws Exception {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        Session session = mock(Session.class);
        when(sqlOperations.createSession(any(String.class), any(User.class))).thenReturn(session);
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new AlwaysOKNullAuthentication(),
                null);
        EmbeddedChannel channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "", "select ?", new int[0]);
        ClientMessages.sendFlush(buffer);

        channel.writeInbound(buffer);

        verify(session, times(1)).sync();
    }

    @Test
    public void testBindMessageCanBeReadIfTypeForParamsIsUnknown() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new AlwaysOKNullAuthentication(),
                null);
        EmbeddedChannel channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "S1", "select ?, ?", new int[0]); // no type hints for parameters

        List<Object> params = Arrays.asList(10, 20);
        ClientMessages.sendBindMessage(buffer, "P1", "S1", params);

        channel.writeInbound(buffer);

        Session session = sessions.get(0);
        // If the query can be retrieved via portalName it means bind worked
        assertThat(session.getQuery("P1"), is("select ?, ?"));
    }

    @Test
    public void testDescribePortalMessage() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new AlwaysOKNullAuthentication(),
                null);

        EmbeddedChannel channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc");
            ClientMessages.sendParseMessage(buffer,
                "S1",
                "select ? in (1, 2, 3)",
                new int[] { PGTypes.get(DataTypes.LONG).oid() });
                ClientMessages.sendBindMessage(buffer,
                    "P1",
                    "S1",
                    Collections.singletonList(1));
            channel.writeInbound(buffer);

            // we're not interested in the startup, parse, or bind replies
            channel.flushOutbound();
            channel.outboundMessages().clear();
        }
        {
            // try portal describe message
            ByteBuf buffer = Unpooled.buffer();
            ClientMessages.sendDescribeMessage(buffer, ClientMessages.DescribeType.PORTAL, "P1");
            channel.writeInbound(buffer);

            // we should get back a RowDescription message
            ByteBuf response = channel.readOutbound();
            assertThat(response.readByte(), is((byte) 'T'));
            assertThat(response.readInt(), is(42));
            assertThat(response.readShort(), is((short) 1));
            assertThat(PostgresWireProtocol.readCString(response), is("($1 IN (1, 2, 3))"));

            assertThat(response.readInt(), is(0));
            assertThat(response.readShort(), is((short) 0));
            assertThat(response.readInt(), is(PGTypes.get(DataTypes.BOOLEAN).oid()));
            assertThat(response.readShort(), is((short) PGTypes.get(DataTypes.BOOLEAN).typeLen()));
            assertThat(response.readInt(), is(PGTypes.get(DataTypes.LONG).typeMod()));
            assertThat(response.readShort(), is((short) 0));
        }
    }

    @Test
    public void testDescribeStatementMessage() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                sqlOperations,
                new AlwaysOKNullAuthentication(),
                null);

        EmbeddedChannel channel = new EmbeddedChannel(ctx.decoder, ctx.handler);
        {
            ByteBuf buffer = Unpooled.buffer();

            ClientMessages.sendStartupMessage(buffer, "doc");
            ClientMessages.sendParseMessage(buffer, "S1", "select ? in (1, 2, 3)", new int[0]);
            channel.writeInbound(buffer);

            // we're not interested in the startup, parse, or bind replies
            channel.flushOutbound();
            channel.outboundMessages().clear();
        }
        {
            // try the describe statement variant
            ByteBuf buffer = Unpooled.buffer();
            ClientMessages.sendDescribeMessage(buffer, ClientMessages.DescribeType.STATEMENT, "S1");
            channel.writeInbound(buffer);

            // we should get back a ParameterDescription message
            ByteBuf response = channel.readOutbound();
            assertThat(response.readByte(), is((byte) 't'));
            assertThat(response.readInt(), is(10));
            assertThat(response.readShort(), is((short) 1));
            assertThat(response.readInt(), is(PGTypes.get(DataTypes.LONG).oid()));

            // we should get back a RowDescription message
            response = channel.readOutbound();
            assertThat(response.readByte(), is((byte) 'T'));
            assertThat(response.readInt(), is(42));
            assertThat(response.readShort(), is((short) 1));
            assertThat(PostgresWireProtocol.readCString(response), is("($1 IN (1, 2, 3))"));

            assertThat(response.readInt(), is(0));
            assertThat(response.readShort(), is((short) 0));
            assertThat(response.readInt(), is(PGTypes.get(DataTypes.BOOLEAN).oid()));
            assertThat(response.readShort(), is((short) PGTypes.get(DataTypes.BOOLEAN).typeLen()));
            assertThat(response.readInt(), is(PGTypes.get(DataTypes.LONG).typeMod()));
            assertThat(response.readShort(), is((short) 0));
        }
    }

    @Test
    public void testSslRejection() {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                new AlwaysOKNullAuthentication(),
                null);

        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendSslReqMessage(buffer);
        channel.writeInbound(buffer);

        // We should get back an 'N'...
        ByteBuf responseBuffer = channel.readOutbound();
        byte response = responseBuffer.readByte();
        assertEquals(response, 'N');

        // ...and continue unencrypted (no ssl handler)
        for (Map.Entry<String, ChannelHandler> entry : channel.pipeline()) {
            assertThat(entry.getValue(), isOneOf(ctx.decoder, ctx.handler));
        }
    }

    @Test
    public void testCrateServerVersionIsReceivedOnStartup() throws Exception {
        PostgresWireProtocol ctx = new PostgresWireProtocol(
            sqlOperations, new AlwaysOKNullAuthentication(), null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf buf = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buf, "doc");
        channel.writeInbound(buf);

        ByteBuf respBuf;
        respBuf = channel.readOutbound();
        assertThat((char) respBuf.readByte(), is('R')); // Auth OK

        respBuf = channel.readOutbound();
        assertThat((char) respBuf.readByte(), is('S')); // ParameterStatus
        respBuf.readInt(); // length
        String key = PostgresWireProtocol.readCString(respBuf);
        String value = PostgresWireProtocol.readCString(respBuf);

        assertThat(key, is("crate_version"));
        assertThat(value, is(Version.CURRENT.toString()));
    }

    @Test
    public void testPasswordMessageAuthenticationProcess() throws Exception {
        PostgresWireProtocol ctx =
            new PostgresWireProtocol(
                mock(SQLOperations.class),
                new Authentication() {
                    @Nullable
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
                null);
        channel = new EmbeddedChannel(ctx.decoder, ctx.handler);

        ByteBuf respBuf;
        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        channel.writeInbound(buffer);

        respBuf = channel.readOutbound();
        assertThat((char) respBuf.readByte(), is('R')); // AuthenticationCleartextPassword

        buffer = Unpooled.buffer();
        ClientMessages.sendPasswordMessage(buffer, "pw");
        channel.writeInbound(buffer);

        respBuf = channel.readOutbound();
        assertThat((char) respBuf.readByte(), is('R')); // Auth OK
    }
}
