/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;

import io.crate.action.FutureActionListener;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class CloseableChannel implements Closeable {

    private final boolean isServerChannel;
    private final Channel channel;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private volatile long lastAccessedTime;

    public CloseableChannel(Channel channel, boolean isServerChannel) {
        this.lastAccessedTime = TimeValue.nsecToMSec(System.nanoTime());
        this.isServerChannel = isServerChannel;
        this.channel = channel;
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                closeFuture.complete(null);
            } else {
                Throwable cause = f.cause();
                closeFuture.completeExceptionally(cause);
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                }
            }
        });
    }

    public ChannelFuture writeAndFlush(ByteBuf byteBuf) {
        return channel.writeAndFlush(byteBuf);
    }

    public boolean isServerChannel() {
        return isServerChannel;
    }

    public void markAccessed(long relativeMillisTime) {
        lastAccessedTime = relativeMillisTime;
    }

    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    /**
     * Adds a listener that will be executed when the channel is closed. If the channel is still open when
     * this listener is added, the listener will be executed by the thread that eventually closes the
     * channel. If the channel is already closed when the listener is added the listener will immediately be
     * executed by the thread that is attempting to add the listener.
     *
     * @param listener to be executed
     */
    public void addCloseListener(ActionListener<Void> listener) {
        closeFuture.whenComplete(listener);
    }

    /**
     * Indicates whether a channel is currently open
     *
     * @return boolean indicating if channel is open
     */
    public boolean isOpen() {
        return channel.isOpen();
    }

    /**
     * @return the local address of this channel.
     */
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public void close() {
        channel.close();
    }

    /**
     * Closes the channel.
     *
     * @param channel  to close
     * @param blocking indicates if we should block on channel close
     */
    public static void closeChannel(CloseableChannel channel, boolean blocking) {
        closeChannels(Collections.singletonList(channel), blocking);
    }

    /**
     * Closes the channels.
     *
     * @param channels to close
     * @param blocking indicates if we should block on channel close
     */
    public static void closeChannels(Collection<? extends CloseableChannel> channels, boolean blocking) {
        try {
            IOUtils.close(channels);
        } catch (IOException e) {
            // The CloseableChannel#close method does not throw IOException, so this should not occur.
            throw new AssertionError(e);
        }
        if (blocking) {
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>(channels.size());
            for (var channel : channels) {
                FutureActionListener<Void, Void> closeFuture = FutureActionListener.newInstance();
                channel.addCloseListener(closeFuture);
                futures.add(closeFuture);
            }
            for (var future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    // Ignore as we are only interested in waiting for the close process to complete. Logging
                    // close exceptions happens elsewhere.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public String toString() {
        return "Channel{"
            + "isServer=" + isServerChannel
            + ", lastAccess=" + lastAccessedTime
            + ", netty=" + channel
            + "}";
    }
}
