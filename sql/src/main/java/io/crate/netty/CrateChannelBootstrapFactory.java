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

package io.crate.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4Transport;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Factory utility for creating channel server bootstraps, based on the relevant netty {@link Settings}
 */
public final class CrateChannelBootstrapFactory {

    private CrateChannelBootstrapFactory() {
    }

    public static ServerBootstrap newChannelBootstrap(String id, Settings settings) {
        EventLoopGroup boss = new NioEventLoopGroup(
            Netty4Transport.NETTY_BOSS_COUNT.get(settings), daemonThreadFactory(settings, id + "-netty-boss"));
        EventLoopGroup worker = new NioEventLoopGroup(
            Netty4Transport.WORKER_COUNT.get(settings), daemonThreadFactory(settings, id + "-netty-worker"));
        Boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        return new ServerBootstrap()
            .channel(NioServerSocketChannel.class)
            .group(boss, worker)
            .option(ChannelOption.SO_REUSEADDR, reuseAddress)
            .childOption(ChannelOption.SO_REUSEADDR, reuseAddress)
            .childOption(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings))
            .childOption(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
    }
}
