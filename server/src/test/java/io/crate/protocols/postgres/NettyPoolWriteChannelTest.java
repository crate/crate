/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.types.StringType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.List;
import java.util.Random;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.is;

public class NettyPoolWriteChannelTest extends ESTestCase {

    private static final Random RANDOM = new Random();

    @Test
    public void test_writes_of_multiple_threads_retain_ordering() throws Exception {
        var embeddedChannel = new EmbeddedChannel();
        var channel = new NettyPoolWriteChannel(new AsyncTestChannel(embeddedChannel));

        Messages.sendParseComplete(channel);
        Messages.sendBindComplete(channel);
        Messages.sendRowDescription(
            channel,
            List.of(createReference("foo", StringType.INSTANCE)),
            null,
            null
        );
        Messages.sendCommandComplete(channel, "SELECT foo FROM t1", 1);
        Messages.sendReadyForQuery(channel, TransactionState.IDLE);
        embeddedChannel.flushOutbound();

        var outQueue = embeddedChannel.outboundMessages();
        //assertThat(outQueue.size(), is(4));
        assertThat((char) ((ByteBuf) outQueue.poll()).getByte(0), is('1'));
        assertThat((char) ((ByteBuf) outQueue.poll()).getByte(0), is('2'));
        assertThat((char) ((ByteBuf) outQueue.poll()).getByte(0), is('T'));
        assertThat((char) ((ByteBuf) outQueue.poll()).getByte(0), is('C'));

    }

    private static class AsyncTestChannel implements Channel {

        private final Channel delegate;

        public AsyncTestChannel(Channel delegate) {
            this.delegate = delegate;
        }

        @Override
        public ChannelFuture write(Object msg) {
            return invokeWrite(msg, newPromise(), false);
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            return invokeWrite(msg, promise, false);
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            return invokeWrite(msg, newPromise(), true);
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return invokeWrite(msg, promise, true);
        }

        private ChannelFuture invokeWrite(Object msg, ChannelPromise promise, boolean flush) {
            if (RANDOM.nextBoolean()) {
                delegate.write(msg, promise);
                if (flush) {
                    delegate.flush();
                }
            } else {
                eventLoop().execute(() -> {
                    delegate.write(msg, promise);
                    if (flush) {
                        delegate.flush();
                    }
                });
            }
            return promise;
        }


        @Override
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return delegate.attr(key);
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return delegate.hasAttr(key);
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return delegate.bind(localAddress);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return delegate.connect(remoteAddress);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return delegate.connect(remoteAddress, localAddress);
        }

        @Override
        public ChannelFuture disconnect() {
            return delegate.disconnect();
        }

        @Override
        public ChannelFuture close() {
            return delegate.close();
        }

        @Override
        public ChannelFuture deregister() {
            return delegate.deregister();
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return delegate.bind(localAddress, promise);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return delegate.connect(remoteAddress, promise);
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return delegate.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return delegate.disconnect(promise);
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return delegate.close(promise);
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return delegate.deregister(promise);
        }

        @Override
        public ChannelPromise newPromise() {
            return delegate.newPromise();
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return delegate.newProgressivePromise();
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return delegate.newSucceededFuture();
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return delegate.newFailedFuture(cause);
        }

        @Override
        public ChannelPromise voidPromise() {
            return delegate.voidPromise();
        }

        @Override
        public int compareTo(Channel o) {
            return delegate.compareTo(o);
        }

        @Override
        public ChannelId id() {
            return delegate.id();
        }

        @Override
        public EventLoop eventLoop() {
            return delegate.eventLoop();
        }

        @Override
        public Channel parent() {
            return delegate.parent();
        }

        @Override
        public ChannelConfig config() {
            return delegate.config();
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public boolean isRegistered() {
            return delegate.isRegistered();
        }

        @Override
        public boolean isActive() {
            return delegate.isActive();
        }

        @Override
        public ChannelMetadata metadata() {
            return delegate.metadata();
        }

        @Override
        public SocketAddress localAddress() {
            return delegate.localAddress();
        }

        @Override
        public SocketAddress remoteAddress() {
            return delegate.remoteAddress();
        }

        @Override
        public ChannelFuture closeFuture() {
            return delegate.closeFuture();
        }

        @Override
        public boolean isWritable() {
            return delegate.isWritable();
        }

        @Override
        public long bytesBeforeUnwritable() {
            return delegate.bytesBeforeUnwritable();
        }

        @Override
        public long bytesBeforeWritable() {
            return delegate.bytesBeforeWritable();
        }

        @Override
        public Unsafe unsafe() {
            return delegate.unsafe();
        }

        @Override
        public ChannelPipeline pipeline() {
            return delegate.pipeline();
        }

        @Override
        public ByteBufAllocator alloc() {
            return delegate.alloc();
        }

        @Override
        public Channel read() {
            return delegate.read();
        }

        @Override
        public Channel flush() {
            return delegate.flush();
        }
    }
}
