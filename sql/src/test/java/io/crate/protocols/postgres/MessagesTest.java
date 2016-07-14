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

import io.crate.core.collections.RowN;
import io.crate.types.DataTypes;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessagesTest {

    @Test
    public void testNullValuesAddToLength() throws Exception {
        Channel channel = mock(Channel.class, Answers.RETURNS_MOCKS.get());
        Messages.sendDataRow(
            channel,
            new RowN($(10, null)),
            Arrays.asList(DataTypes.INTEGER, DataTypes.STRING), null
        );
        ArgumentCaptor<Object> writeCaptor = ArgumentCaptor.forClass(Object.class);
        verify(channel).write(writeCaptor.capture());
        ChannelBuffer buffer = (ChannelBuffer) writeCaptor.getValue();

        // message type
        assertThat((char) buffer.readByte(), is('D'));

         // size of the message
        assertThat(buffer.readInt(), is(16));
        assertThat(buffer.readableBytes(), is(12)); // 16 - INT4 because the size was already read
    }
}
