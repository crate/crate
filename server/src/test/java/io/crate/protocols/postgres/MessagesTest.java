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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

public class MessagesTest extends ESTestCase {

    @Test
    public void testBufferInSendDataRowIsReleasedIfGetValueFromRowFails() {
        Channel channel = mock(Channel.class);
        ByteBufAllocator byteBufAllocator = mock(ByteBufAllocator.class);
        ByteBuf buf = Unpooled.buffer();
        when(byteBufAllocator.buffer()).thenReturn(buf);
        when(channel.alloc()).thenReturn(byteBufAllocator);
        try {
            Messages.sendDataRow(
                channel,
                new Row() {
                    @Override
                    public int numColumns() {
                        return 1;
                    }

                    @Override
                    public Object get(int index) {
                        throw new IllegalArgumentException("Dummy");
                    }
                },
                Collections.singletonList(PGTypes.get(DataTypes.INTEGER)),
                null
            );
            fail("sendDataRow should raise an exception");
        } catch (Exception ignored) {
        }
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    public void testNullValuesAddToLength() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        Messages.sendDataRow(
            channel,
            new RowN($(10, null)),
            Arrays.asList(PGTypes.get(DataTypes.INTEGER), PGTypes.get(DataTypes.STRING)), null
        );
        channel.flush();
        ByteBuf buffer = channel.readOutbound();

        try {
            // message type
            assertThat((char) buffer.readByte()).isEqualTo('D');

            // size of the message
            assertThat(buffer.readInt()).isEqualTo(16);
            assertThat(buffer.readableBytes()).isEqualTo(12); // 16 - INT4 because the size was already read
        } finally {
            buffer.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testCommandCompleteWithWhitespace() throws Exception {
        final EmbeddedChannel channel = new EmbeddedChannel();
        try {
            final String response = "SELECT 42";

            Messages.sendCommandComplete(channel, "Select 1", 42);
            channel.flush();
            verifyResponse(channel, response);
            Messages.sendCommandComplete(channel, " Select 1", 42);
            channel.flush();
            verifyResponse(channel, response);
            Messages.sendCommandComplete(channel, "  Select 1 ", 42);
            channel.flush();
            verifyResponse(channel, response);
            Messages.sendCommandComplete(channel, "\n  Select 1", 42);
            channel.flush();
            verifyResponse(channel, response);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static void verifyResponse(EmbeddedChannel channel, String response) {
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        ByteBuf buffer = (ByteBuf) channel.outboundMessages().poll();
        try {
            assertThat(buffer.readByte()).isEqualTo((byte) 'C');
            assertThat(buffer.readInt()).isEqualTo(responseBytes.length + 4 + 1);
            byte[] string = new byte[9];
            buffer.readBytes(string);
            assertThat(string).isEqualTo(responseBytes);
        } finally {
            buffer.release();
        }
    }
}
