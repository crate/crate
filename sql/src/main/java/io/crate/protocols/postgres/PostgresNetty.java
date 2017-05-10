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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.annotations.VisibleForTesting;
import io.crate.action.sql.SQLOperations;
import io.crate.operation.auth.Authentication;
import io.crate.operation.auth.AuthenticationProvider;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.transport.BindTransportException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

@Singleton
public class PostgresNetty extends AbstractLifecycleComponent {

    public static final CrateSetting<Boolean> PSQL_ENABLED_SETTING = CrateSetting.of(Setting.boolSetting(
        "psql.enabled", true,
        Setting.Property.NodeScope), DataTypes.BOOLEAN);
    public static final CrateSetting<String> PSQL_PORT_SETTING = CrateSetting.of(new Setting<>(
        "psql.port", "5432-5532",
        Function.identity(), Setting.Property.NodeScope), DataTypes.STRING);

    private final SQLOperations sqlOperations;
    private final NetworkService networkService;

    private final boolean enabled;
    private final String port;
    private final AuthenticationProvider authProvider;
    private final Logger namedLogger;

    private ServerBootstrap bootstrap;

    private final List<Channel> serverChannels = new ArrayList<>();
    private final List<InetSocketTransportAddress> boundAddresses = new ArrayList<>();
    @Nullable
    private  BoundTransportAddress boundAddress = null;

    @Inject
    public PostgresNetty(Settings settings,
                         SQLOperations sqlOperations,
                         NetworkService networkService,
                         AuthenticationProvider authProvider) {
        super(settings);
        namedLogger = Loggers.getLogger("psql", settings);
        this.sqlOperations = sqlOperations;
        this.networkService = networkService;
        this.authProvider = authProvider;

        enabled = PSQL_ENABLED_SETTING.setting().get(settings);
        port = PSQL_PORT_SETTING.setting().get(settings);
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
        Authentication authService = authProvider.get();
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(daemonThreadFactory(settings, "postgres-netty-boss")),
            Executors.newCachedThreadPool(daemonThreadFactory(settings, "postgres-netty-worker"))
        ));
        bootstrap.setOption("child.tcpNoDelay", settings.getAsBoolean("tcp_no_delay", true));
        bootstrap.setOption("child.keepAlive", settings.getAsBoolean("tcp_keep_alive", true));
        bootstrap.setPipelineFactory(() -> {
            ChannelPipeline pipeline = Channels.pipeline();
            ConnectionContext connectionContext = new ConnectionContext(sqlOperations, authService);
            pipeline.addLast("frame-decoder", connectionContext.decoder);
            pipeline.addLast("handler", connectionContext.handler);
            return pipeline;
        });

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


    static int resolvePublishPort(List<InetSocketTransportAddress> boundAddresses, InetAddress publishInetAddress) {
        for (InetSocketTransportAddress boundAddress : boundAddresses) {
            InetAddress boundInetAddress = boundAddress.address().getAddress();
            if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                return boundAddress.getPort();
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        final IntSet ports = new IntHashSet();
        for (InetSocketTransportAddress boundAddress : boundAddresses) {
            ports.add(boundAddress.getPort());
        }
        if (ports.size() == 1) {
            return ports.iterator().next().value;
        }

        throw new BindHttpException("Failed to auto-resolve psql publish port, multiple bound addresses " + boundAddresses +
                                    " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). ");
    }

    private BoundTransportAddress resolveBindAddress() {
        // Bind and start to accept incoming connections.
        try {
            InetAddress[] hostAddresses = networkService.resolveBindHostAddresses(null);
            for (InetAddress address : hostAddresses) {
                if (address instanceof Inet4Address) {
                    boundAddresses.add(bindAddress(address));
                }
            }
        } catch (IOException e) {
            throw new BindPostgresException("Failed to resolve binding network host", e);
        }
        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(null);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        final int publishPort = resolvePublishPort(boundAddresses, publishInetAddress);
        final InetSocketAddress publishAddress = new InetSocketAddress(publishInetAddress, publishPort);
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[boundAddresses.size()]), new InetSocketTransportAddress(publishAddress));
    }

    private InetSocketTransportAddress bindAddress(final InetAddress hostAddress) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                Channel channel = bootstrap.bind(new InetSocketAddress(hostAddress, portNumber));
                serverChannels.add(channel);
                boundSocket.set((InetSocketAddress) channel.getLocalAddress());
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindPostgresException("Failed to bind to [" + port + "]", lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound psql to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new InetSocketTransportAddress(boundSocket.get());
    }

    @Override
    protected void doStop() {
        for (Channel channel : serverChannels) {
            channel.close().awaitUninterruptibly();
        }
        if (bootstrap != null) {
            bootstrap.releaseExternalResources();
            bootstrap = null;
        }
    }

    @Override
    protected void doClose() {
    }

    @VisibleForTesting
    public Collection<InetSocketTransportAddress> boundAddresses() {
        return Collections.unmodifiableCollection(boundAddresses);
    }
}
