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
import io.crate.action.sql.SessionContext;
import io.crate.executor.Executor;
import io.crate.operation.auth.Authentication;
import io.crate.operation.auth.AuthenticationMethod;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.operation.auth.HbaProtocol;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.user.User;
import io.crate.protocols.postgres.ssl.SslHandler;
import io.crate.protocols.postgres.ssl.SslHandlerUtils;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.netty.util.ReferenceCountUtil.releaseLater;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class ConnectionContextTest extends CrateDummyClusterServiceUnitTest {

    private SQLOperations sqlOperations;
    private List<SQLOperations.Session> sessions = new ArrayList<>();

    @Before
    public void prepare() {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        sqlOperations = new SQLOperations(
            e.analyzer,
            e.planner,
            () -> mock(Executor.class),
            new JobsLogs(() -> true),
            Settings.EMPTY,
            clusterService
        ) {
            @Override
            public Session createSession(SessionContext sessionContext) {
                Session session = super.createSession(sessionContext);
                sessions.add(session);
                return session;
            }
        };
    }

    @Test
    public void testHandleEmptySimpleQuery() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionContext ctx =
            ConnectionContext.setup(
                channel,
                SslHandlerUtils.getDefault(),
                mock(SQLOperations.class),
                AuthenticationProvider.NOOP_AUTH);

        ByteBuf buffer = releaseLater(Unpooled.buffer());
        Messages.writeCString(buffer, ";".getBytes(StandardCharsets.UTF_8));
        ctx.handleSimpleQuery(buffer, channel);

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

        channel.close().awaitUninterruptibly();
    }

    @Test
    public void testFlushMessageResultsInSyncCallOnSession() throws Exception {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        SQLOperations.Session session = mock(SQLOperations.Session.class);
        when(sqlOperations.createSession(any(SessionContext.class))).thenReturn(session);
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionContext.setup(
            channel,
            SslHandlerUtils.getDefault(),
            sqlOperations,
            new TestAuthentication(null));


        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "", "select ?", new int[0]);
        ClientMessages.sendFlush(buffer);

        channel.writeInbound(buffer);

        verify(session, times(1)).sync();
        channel.close().awaitUninterruptibly();
    }

    @Test
    public void testBindMessageCanBeReadIfTypeForParamsIsUnknown() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        ConnectionContext.setup(
            channel,
            SslHandlerUtils.getDefault(),
            sqlOperations,
            new TestAuthentication(null));

        ByteBuf buffer = Unpooled.buffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "S1", "select ?, ?", new int[0]); // no type hints for parameters

        List<Object> params = Arrays.asList(10, 20);
        ClientMessages.sendBindMessage(buffer, "P1", "S1", params);

        channel.writeInbound(buffer);

        SQLOperations.Session session = sessions.get(0);
        // If the query can be retrieved via portalName it means bind worked
        assertThat(session.getQuery("P1"), is("select ?, ?"));

        channel.close().awaitUninterruptibly();
    }

    private static class TestAuthentication implements Authentication {

        private final User user;

        TestAuthentication(User user) {
            this.user = user;
        }

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public AuthenticationMethod resolveAuthenticationType(String user, InetAddress address, HbaProtocol protocol) {
            return new AuthenticationMethod() {
                @Nullable
                @Override
                public User authenticate(String userName) {
                    return TestAuthentication.this.user;
                }

                @Override
                public String name() {
                    return null;
                }
            };
        }
    }
}
