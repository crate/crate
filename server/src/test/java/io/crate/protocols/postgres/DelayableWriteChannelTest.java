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

import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

public class DelayableWriteChannelTest extends ESTestCase {

    @Test
    public void test_delayed_writes_are_released_on_close() throws Exception {
        var channel = new DelayableWriteChannel(new EmbeddedChannel());
        channel.delayWrites();
        ByteBuf buffer = Unpooled.buffer();
        channel.write(buffer);
        channel.close();
        assertThat(buffer.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_can_add_and_unblock_from_different_threads() throws Exception {
        AtomicInteger numMessages = new AtomicInteger(50);
        EmbeddedChannel innerChannel = new EmbeddedChannel();
        var channel = new DelayableWriteChannel(innerChannel);
        try {
            channel.delayWrites();
            var thread1 = new Thread(() -> {
                while (numMessages.decrementAndGet() >= 0) {
                    ByteBuf msg = channel.alloc().buffer();
                    msg.setInt(0, 1);
                    channel.write(msg);
                }
            });
            var thread2 = new Thread(() -> {
                while (numMessages.get() > 0) {
                    channel.writePendingMessages();
                    channel.delayWrites();
                }
            });
            thread1.start();
            thread2.start();

            thread1.join();
            thread2.join();

            channel.writePendingMessages();
            channel.flush();
            assertThat(innerChannel.outboundMessages()).hasSize(50);
        } finally {
            innerChannel.finishAndReleaseAll();
        }
    }
}
