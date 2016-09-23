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
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class ConnectionContextTest {

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
}
