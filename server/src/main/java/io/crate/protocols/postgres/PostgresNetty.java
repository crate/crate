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

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.StatsTracker;
import org.elasticsearch.transport.netty4.Netty4InboundStatsHandler;
import org.elasticsearch.transport.netty4.Netty4MessageChannelHandler;
import org.elasticsearch.transport.netty4.Netty4OutboundStatsHandler;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;

import io.crate.session.Sessions;
import io.crate.auth.Authentication;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ConnectionStats;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.protocols.ssl.SslSettings;
import io.crate.role.Roles;
import io.crate.types.DataTypes;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;

@Singleton
public class PostgresNetty extends AbstractLifecycleComponent {

    private static final Logger LOGGER = LogManager.getLogger(PostgresNetty.class);

    public static final Setting<Boolean> PSQL_ENABLED_SETTING = Setting.boolSetting(
        "psql.enabled", true, Setting.Property.NodeScope);

    // Explicit generic is required for eclipse JDT, otherwise it won't compile
    public static final Setting<String> PSQL_PORT_SETTING = new Setting<>(
        "psql.port",
        "5432-5532",
        Function.identity(),
        DataTypes.STRING,
        Setting.Property.NodeScope);

    private final Sessions sqlOperations;
    private final NetworkService networkService;

    private final boolean enabled;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private final String port;
    private final Authentication authentication;
    private final NettyBootstrap nettyBootstrap;
    private final Logger namedLogger;
    private final Settings settings;
    private final Roles roles;
    private final Supplier<SslContext> sslContextProvider;

    private ServerBootstrap bootstrap;

    private final List<Channel> serverChannels = new ArrayList<>();
    private final List<TransportAddress> boundAddresses = new ArrayList<>();
    @Nullable
    private BoundTransportAddress boundAddress;
    private final StatsTracker statsTracker = new StatsTracker();
    @Nullable
    private Netty4InboundStatsHandler inboundStatsHandler;
    @Nullable
    private Netty4OutboundStatsHandler outboundStatsHandler;

    private final PageCacheRecycler pageCacheRecycler;
    private final Netty4Transport transport;
    private final SessionSettingRegistry sessionSettingRegistry;

    @Inject
    public PostgresNetty(Settings settings,
                         SessionSettingRegistry sessionSettingRegistry,
                         Sessions sqlOperations,
                         Roles roles,
                         NetworkService networkService,
                         Authentication authentication,
                         NettyBootstrap nettyBootstrap,
                         Netty4Transport netty4Transport,
                         PageCacheRecycler pageCacheRecycler,
                         SslContextProvider sslContextProvider) {
        this.settings = settings;
        this.sessionSettingRegistry = sessionSettingRegistry;
        this.roles = roles;
        namedLogger = LogManager.getLogger("psql");
        this.sqlOperations = sqlOperations;
        this.networkService = networkService;
        this.authentication = authentication;
        this.nettyBootstrap = nettyBootstrap;
        this.transport = netty4Transport;
        this.pageCacheRecycler = pageCacheRecycler;

        if (SslSettings.isPSQLSslEnabled(settings)) {
            namedLogger.info("PSQL SSL support is enabled.");
            this.sslContextProvider = sslContextProvider;
        } else {
            namedLogger.info("PSQL SSL support is disabled.");
            this.sslContextProvider = () -> null;
        }

        enabled = PSQL_ENABLED_SETTING.get(settings);
        bindHosts = NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings).toArray(new String[0]);
        publishHosts = NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings).toArray(new String[0]);
        port = PSQL_PORT_SETTING.get(settings);
    }

    @Nullable
    public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

    @Override
    protected void doStart() {
        if (!enabled) {
            return;
        }
        var eventLoopGroup = nettyBootstrap.getSharedEventLoopGroup();
        bootstrap = NettyBootstrap.newServerBootstrap(settings, eventLoopGroup);
        inboundStatsHandler = new Netty4InboundStatsHandler(statsTracker, LOGGER);
        outboundStatsHandler = new Netty4OutboundStatsHandler(statsTracker, LOGGER);

        bootstrap.childHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("inbound_stats", PostgresNetty.this.inboundStatsHandler);
                pipeline.addLast("outbound_stats", PostgresNetty.this.outboundStatsHandler);
                PostgresWireProtocol postgresWireProtocol = new PostgresWireProtocol(
                    sqlOperations,
                    sessionSettingRegistry,
                    coordinatorSessionSettings ->
                        roles.getAccessControl(
                            coordinatorSessionSettings.authenticatedUser(),
                            coordinatorSessionSettings.sessionUser()
                        ),
                    chPipeline -> {
                        var nettyTcpChannel = new CloseableChannel(ch, true);
                        ch.attr(Netty4Transport.CHANNEL_KEY).set(nettyTcpChannel);
                        chPipeline.addLast("dispatcher", new Netty4MessageChannelHandler(pageCacheRecycler, transport));
                    },
                    authentication,
                    sslContextProvider
                );
                pipeline.addLast("frame-decoder", postgresWireProtocol.decoder);
                pipeline.addLast("handler", postgresWireProtocol.handler);
            }
        });
        bootstrap.validate();

        boolean success = false;
        try {
            boundAddress = resolveBindAddress();
            namedLogger.info("{}", boundAddress);
            success = true;
        } finally {
            if (!success) {
                doStop(); // stop boss/worker threads to avoid leaks
            }
        }
    }

    static int resolvePublishPort(List<TransportAddress> boundAddresses, InetAddress publishInetAddress) {
        for (TransportAddress boundAddress : boundAddresses) {
            InetAddress boundInetAddress = boundAddress.address().getAddress();
            if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                return boundAddress.getPort();
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        final IntSet ports = new IntHashSet();
        for (TransportAddress boundAddress : boundAddresses) {
            ports.add(boundAddress.getPort());
        }
        if (ports.size() == 1) {
            return ports.iterator().next().value;
        }

        throw new BindHttpException(
            "Failed to auto-resolve psql publish port, multiple bound addresses " + boundAddresses +
                " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). ");
    }

    private BoundTransportAddress resolveBindAddress() {
        // Bind and start to accept incoming connections.
        try {
            List<InetAddress> hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
            for (InetAddress address : hostAddresses) {
                if (address instanceof Inet4Address || address instanceof Inet6Address) {
                    boundAddresses.add(bindAddress(address));
                }
            }
        } catch (IOException e) {
            throw new BindPostgresException("Failed to resolve binding network host", e);
        }
        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        final int publishPort = resolvePublishPort(boundAddresses, publishInetAddress);
        final InetSocketAddress publishAddress = new InetSocketAddress(publishInetAddress, publishPort);
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), new TransportAddress(publishAddress));
    }

    private TransportAddress bindAddress(final InetAddress hostAddress) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                Channel channel = bootstrap.bind(new InetSocketAddress(hostAddress, portNumber)).sync().channel();
                serverChannels.add(channel);
                boundSocket.set((InetSocketAddress) channel.localAddress());
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindPostgresException("Failed to bind to [" + port + "]", lastException.get());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Bound psql to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new TransportAddress(boundSocket.get());
    }

    @Override
    protected void doStop() {
        for (Channel channel : serverChannels) {
            channel.close().awaitUninterruptibly();
        }
        serverChannels.clear();
        if (bootstrap != null) {
            bootstrap = null;
        }
        if (inboundStatsHandler != null) {
            inboundStatsHandler.close();
            inboundStatsHandler = null;
        }
        if (outboundStatsHandler != null) {
            outboundStatsHandler.close();
            outboundStatsHandler = null;
        }
    }

    @Override
    protected void doClose() {
        // nothing to close here, see doStop()
    }

    public ConnectionStats stats() {
        return statsTracker.stats();
    }
}
