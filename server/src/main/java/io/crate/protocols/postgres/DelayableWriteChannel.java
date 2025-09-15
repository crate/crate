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

import java.net.SocketAddress;
import java.util.ArrayDeque;

import org.jetbrains.annotations.Nullable;

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
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

/**
 * Channel implementation that allows to delay writes with `blockWritesUntil`
 **/
public class DelayableWriteChannel implements Channel {

    private final Channel delegate;
    private DelayedWrites delay = null;

    public DelayableWriteChannel(Channel channel) {
        this.delegate = channel;
        channel.closeFuture().addListener(_ -> discardDelayedWrites());
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
    public ChannelFuture write(Object msg) {
        return this.write(msg, newPromise());
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        return this.writeAndFlush(msg, newPromise());
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

    public Channel bypassDelay() {
        return delegate;
    }


    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        synchronized (this) {
            if (delay != null) {
                delay.add(msg, () -> delegate.write(msg, promise));
                return promise;
            }
        }
        return delegate.write(msg, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        synchronized (this) {
            if (delay != null) {
                delay.add(msg, () -> delegate.writeAndFlush(msg, promise));
                return promise;
            }
        }
        return delegate.writeAndFlush(msg, promise);
    }

    public synchronized void discardDelayedWrites() {
        if (delay != null) {
            var parent = delay.parent;
            while (parent != null) {
                parent.discard();
                parent = parent.parent;
            }
            delay.discard();
            delay = null;
        }
    }

    public synchronized void writePendingMessages(DelayedWrites delayedWrites) {
        if (delay == delayedWrites) {
            delay = null;
        }
        delayedWrites.writeDelayed();
    }

    public synchronized void writePendingMessages() {
        if (delay != null) {
            var parent = delay.parent;
            while (parent != null) {
                parent.writeDelayed();
                parent = parent.parent;
            }
            delay.writeDelayed();
            delay = null;
        }
    }

    public DelayedWrites delayWrites() {
        DelayedWrites delayedWrites = new DelayedWrites(delay);
        delay = delayedWrites;
        return delayedWrites;
    }

    record DelayedMsg(Object msg, Runnable runnable) {
    }

    static class DelayedWrites {

        private final ArrayDeque<DelayedMsg> delayed = new ArrayDeque<>();
        private final DelayedWrites parent;

        public DelayedWrites(@Nullable DelayedWrites parent) {
            this.parent = parent;
        }

        public void discard() {
            DelayedMsg delayedMsg;
            while ((delayedMsg = delayed.poll()) != null) {
                ReferenceCountUtil.safeRelease(delayedMsg.msg);
            }
        }

        public void add(Object msg, Runnable runnable) {
            delayed.add(new DelayedMsg(msg, runnable));
        }

        private void writeDelayed() {
            DelayedMsg delayedMsg;
            while ((delayedMsg = delayed.poll()) != null) {
                delayedMsg.runnable.run();
            }
        }
    }
}
