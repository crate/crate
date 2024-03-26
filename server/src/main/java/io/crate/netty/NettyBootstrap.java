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

package io.crate.netty;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.Netty4Utils;

import org.jetbrains.annotations.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

@Singleton
public class NettyBootstrap extends AbstractLifecycleComponent {

    public static final String WORKER_THREAD_PREFIX = "netty-worker";
    private static final Logger LOGGER = LogManager.getLogger(NettyBootstrap.class);

    private final Settings settings;
    private EventLoopGroup worker;

    public NettyBootstrap(Settings settings) {
        this.settings = settings;
        Netty4Utils.setAvailableProcessors(EsExecutors.PROCESSORS_SETTING.get(settings));
    }

    @VisibleForTesting
    public boolean workerIsShutdown() {
        synchronized (this) {
            return worker == null || worker.isShutdown();
        }
    }

    public synchronized EventLoopGroup getSharedEventLoopGroup() {
        if (worker == null) {
            throw new IllegalStateException("NettyBootstrap wasn't started yet or is already closed");
        }
        return worker;
    }

    public static EventLoopGroup newEventLoopGroup(Settings settings) {
        ThreadFactory workerThreads = daemonThreadFactory(settings, WORKER_THREAD_PREFIX);
        int workerCount = Netty4Transport.WORKER_COUNT.get(settings);
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(workerCount, workerThreads);
        } else {
            return new NioEventLoopGroup(workerCount, workerThreads);
        }
    }

    public static Class<? extends Channel> clientChannel() {
        if (Epoll.isAvailable()) {
            return EpollSocketChannel.class;
        } else {
            return NioSocketChannel.class;
        }
    }

    public static Class<? extends ServerSocketChannel> serverChannel() {
        if (Epoll.isAvailable()) {
            return EpollServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    public static ServerBootstrap newServerBootstrap(Settings settings, EventLoopGroup eventLoopGroup) {
        var serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(serverChannel());
        Boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        return serverBootstrap
            .group(eventLoopGroup)
            .option(ChannelOption.SO_REUSEADDR, reuseAddress)
            .childOption(ChannelOption.SO_REUSEADDR, reuseAddress)
            .childOption(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings))
            .childOption(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
    }

    @Override
    protected void doStart() {
        synchronized (this) {
            worker = newEventLoopGroup(settings);
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        synchronized (this) {
            Future<?> shutdownGracefully = worker.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            shutdownGracefully.awaitUninterruptibly();
            if (shutdownGracefully.isSuccess() == false) {
                LOGGER.warn("Error closing netty event loop group", shutdownGracefully.cause());
            }
            worker = null;
        }
    }
}
