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

import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.executor.Executor;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
            public Session createSession(@Nullable String defaultSchema, @Nullable String userName, Set<Option> options, int defaultLimit) {
                Session session = super.createSession(defaultSchema, userName, options, defaultLimit);
                sessions.add(session);
                return session;
            }
        };
    }

    @Test
    public void testHandleEmptySimpleQuery() throws Exception {
        ConnectionContext ctx = new ConnectionContext(mock(SQLOperations.class));

        ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();
        Messages.writeCString(channelBuffer, ";".getBytes(StandardCharsets.UTF_8));
        Channel channel = mock(Channel.class);

        ctx.handleSimpleQuery(channelBuffer, channel);

        ArgumentCaptor<ChannelBuffer> argumentCaptor = ArgumentCaptor.forClass(ChannelBuffer.class);
        // once for EmptyQueryResponse and a second time for ReadyForQuery
        verify(channel, times(2)).write(argumentCaptor.capture());

        ChannelBuffer firstResponse = argumentCaptor.getAllValues().get(0);
        byte[] responseBytes = new byte[5];
        firstResponse.readBytes(responseBytes);
        assertThat(responseBytes, is(new byte[]{'I', 0, 0, 0, 4}));
    }

    @Test
    public void testFlushMessageResultsInSyncCallOnSession() throws Exception {
        SQLOperations sqlOperations = mock(SQLOperations.class);
        SQLOperations.Session session = mock(SQLOperations.Session.class);
        when(sqlOperations.createSession(anyString(), anyString(), anySetOf(Option.class), anyInt())).thenReturn(session);
        ConnectionContext ctx = new ConnectionContext(sqlOperations);
        DecoderEmbedder<ChannelBuffer> e = new DecoderEmbedder<>(ctx.decoder, ctx.handler);

        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "", "select ?", new int[0]);
        ClientMessages.sendFlush(buffer);
        e.offer(buffer);

        verify(session, times(1)).sync();
    }

    @Test
    public void testBindMessageCanBeReadIfTypeForParamsIsUnknown() throws Exception {
        ConnectionContext ctx = new ConnectionContext(sqlOperations);
        DecoderEmbedder<ChannelBuffer> e = new DecoderEmbedder<>(ctx.decoder, ctx.handler);

        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        ClientMessages.sendStartupMessage(buffer, "doc");
        ClientMessages.sendParseMessage(buffer, "S1", "select ?, ?", new int[0]); // no type hints for parameters

        List<Object> params = Arrays.asList(10, 20);
        ClientMessages.sendBindMessage(buffer, "P1", "S1", params);

        e.offer(buffer);

        SQLOperations.Session session = sessions.get(0);
        // If the query can be retrieved via portalName it means bind worked
        assertThat(session.getQuery("P1"), is("select ?, ?"));
    }
}
