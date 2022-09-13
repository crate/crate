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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

/**
 * Channel implementation that allows to delay writes with `blockWritesUntil`
 **/
public class DelayableWriteChannel implements Channel {

    private static final Logger LOGGER = LogManager.getLogger(DelayableWriteChannel.class);
    private final Channel delegate;
    private final AtomicReference<DelayedWrites> delay = new AtomicReference<>(null);

    public DelayableWriteChannel(Channel channel) {
        this.delegate = channel;
        channel.closeFuture().addListener(f -> {
            discardDelayedWrites();
        });
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
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        DelayedWrites currentDelay = delay.get();
        if (currentDelay != null) {
            if (LOGGER.isDebugEnabled()) {
                if (msg instanceof ByteBuf buf) {
                    byte msgType = buf.getByte(0);
                    LOGGER.debug("Deferring msg={}", (char) msgType);
                }
            }
            currentDelay.add(msg, () -> delegate.write(msg, promise));
            return promise;
        }
        return delegate.write(msg, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        DelayedWrites currentDelay = delay.get();
        if (currentDelay != null) {
            currentDelay.add(msg, () -> delegate.writeAndFlush(msg, promise));
            return promise;
        }
        return delegate.writeAndFlush(msg, promise);
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

    public void discardDelayedWrites() {
        DelayedWrites currentDelay = delay.getAndSet(null);
        if (currentDelay != null) {
            var parent = currentDelay.parent;
            while (parent != null) {
                parent.discard();
                parent = parent.parent;
            }
            currentDelay.discard();
        }
    }

    public void writePendingMessages() {
        LOGGER.debug("writePendingMessages");
        int num = 0;
        DelayedWrites currentDelay = delay.getAndSet(null);
        if (currentDelay != null) {
            var parent = currentDelay.parent;
            while (parent != null) {
                num++;
                parent.writeDelayed();
                parent = parent.parent;
            }
            num++;
            currentDelay.writeDelayed();
        }
        if (num > 0) {
            LOGGER.debug("writePendingMessages triggered {} writeDelayed");
        }
    }

    public void delayWrites() {
        delay.updateAndGet(DelayedWrites::new);
    }

    public void delayWritesUntil(CompletableFuture<?> future) {
        DelayedWrites currentDelay = delay.updateAndGet(DelayedWrites::new);
        future.whenComplete((res, err) -> {
            int numWritten = currentDelay.writeDelayed();
            delay.compareAndSet(currentDelay, null);
            if (numWritten > 0) {
                LOGGER.debug("force flush");
                delegate.flush();
            }
        });
    }

    static class DelayedMsg {
        final Runnable runnable;
        final Object msg;

        public DelayedMsg(Object msg, Runnable runnable) {
            this.runnable = runnable;
            this.msg = msg;
        }
    }

    static class DelayedWrites {

        private final ArrayDeque<DelayedMsg> delayed = new ArrayDeque<>();
        private final DelayedWrites parent;

        public DelayedWrites(@Nullable DelayedWrites parent) {
            this.parent = parent;
        }

        public void discard() {
            DelayedMsg delayedMsg;
            LOGGER.debug("Discarding delayed messages");
            synchronized (delayed) {
                while ((delayedMsg = delayed.poll()) != null) {
                    ReferenceCountUtil.safeRelease(delayedMsg.msg);
                }
            }
        }

        public void add(Object msg, Runnable runnable) {
            synchronized (delayed) {
                delayed.add(new DelayedMsg(msg, runnable));
            }
        }

        private int writeDelayed() {
            LOGGER.debug("Writing delayed messages");
            DelayedMsg delayedMsg;
            synchronized (delayed) {
                int num = 0;
                while ((delayedMsg = delayed.poll()) != null) {
                    delayedMsg.runnable.run();
                    num++;
                }
                LOGGER.debug("Wrote {} delayed messages", num);
                return num;
            }
        }
    }
}
