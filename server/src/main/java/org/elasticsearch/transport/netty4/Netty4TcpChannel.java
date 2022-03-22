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

package org.elasticsearch.transport.netty4;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportException;

import io.crate.concurrent.CompletableContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;

public class Netty4TcpChannel implements TcpChannel {

    private final Channel channel;
    private final boolean isServer;
    private final String profile;
    private final CompletableContext<Void> connectContext;
    private final CompletableFuture<Void> closeContext = new CompletableFuture<>();
    private final ChannelStats stats = new ChannelStats();

    public Netty4TcpChannel(Channel channel, boolean isServer, String profile, @Nullable ChannelFuture connectFuture) {
        this.channel = channel;
        this.isServer = isServer;
        this.profile = profile;
        this.connectContext = new CompletableContext<>();
        this.channel.closeFuture().addListener(f -> {
            if (f.isSuccess()) {
                closeContext.complete(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    closeContext.completeExceptionally(cause);
                } else {
                    closeContext.completeExceptionally(cause);
                }
            }
        });

        connectFuture.addListener(f -> {
            if (f.isSuccess()) {
                connectContext.complete(null);
            } else {
                Throwable cause = f.cause();
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    connectContext.completeExceptionally(new Exception(cause));
                } else {
                    connectContext.completeExceptionally((Exception) cause);
                }
            }
        });
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public boolean isServerChannel() {
        return isServer;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.whenComplete(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        connectContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public ChannelStats getChannelStats() {
        return stats;
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        ChannelPromise writePromise = channel.newPromise();
        writePromise.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                final Throwable cause = f.cause();
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                assert cause instanceof Exception;
                listener.onFailure((Exception) cause);
            }
        });
        channel.writeAndFlush(Netty4Utils.toByteBuf(reference), writePromise);

        if (channel.eventLoop().isShutdown()) {
            listener.onFailure(new TransportException("Cannot send message, event loop is shutting down."));
        }
    }

    @Override
    public String toString() {
        return "Netty4TcpChannel{" +
            "localAddress=" + getLocalAddress() +
            ", remoteAddress=" + channel.remoteAddress() +
            '}';
    }
}
