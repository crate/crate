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

import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;

import io.crate.auth.AuthSettings;
import io.crate.auth.Authentication;
import io.crate.auth.Protocol;
import io.crate.common.SuppressForbidden;
import io.crate.common.exceptions.Exceptions;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.protocols.ssl.SslSettings;
import io.crate.protocols.ssl.SslSettings.SSLMode;
import io.crate.types.DataTypes;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;

/**
 * There are 4 types of connections per node, low/med/high/ping. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High for things like cluster state. Ping is reserved for
 * sending out ping requests to other nodes.
 */
public class Netty4Transport extends TcpTransport {

    public static final Setting<Integer> WORKER_COUNT =
        new Setting<>("transport.netty.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s)),
            (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"), DataTypes.INTEGER, Property.NodeScope);

    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = Setting.byteSizeSetting(
        "transport.netty.receive_predictor_size", new ByteSizeValue(64, ByteSizeUnit.KB), Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN =
        byteSizeSetting("transport.netty.receive_predictor_min", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX =
        byteSizeSetting("transport.netty.receive_predictor_max", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<Integer> NETTY_BOSS_COUNT =
        intSetting("transport.netty.boss_count", 1, 1, Property.NodeScope);


    private final RecvByteBufAllocator recvByteBufAllocator;
    private final ByteSizeValue receivePredictorMin;
    private final ByteSizeValue receivePredictorMax;
    private volatile Bootstrap clientBootstrap;
    private volatile ServerBootstrap serverBootstrap;
    private final NettyBootstrap nettyBootstrap;
    private final SslContextProvider sslContextProvider;
    private final Authentication authentication;

    private final LoggingHandler loggingHandler = new LoggingHandler(LogLevel.TRACE);



    public Netty4Transport(Settings settings,
                           Version version,
                           ThreadPool threadPool,
                           NetworkService networkService,
                           PageCacheRecycler pageCacheRecycler,
                           NamedWriteableRegistry namedWriteableRegistry,
                           CircuitBreakerService circuitBreakerService,
                           NettyBootstrap nettyBootstrap,
                           Authentication authentication,
                           SslContextProvider sslContextProvider) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        this.authentication = authentication;
        this.nettyBootstrap = nettyBootstrap;
        this.sslContextProvider = sslContextProvider;

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.getBytes() == receivePredictorMin.getBytes()) {
            recvByteBufAllocator = new FixedRecvByteBufAllocator((int) receivePredictorMax.getBytes());
        } else {
            recvByteBufAllocator = new AdaptiveRecvByteBufAllocator((int) receivePredictorMin.getBytes(),
                (int) receivePredictorMin.getBytes(), (int) receivePredictorMax.getBytes());
        }
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            var eventLoopGroup = nettyBootstrap.getSharedEventLoopGroup();
            clientBootstrap = createClientBootstrap(eventLoopGroup);
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                createServerBootstrap(settings, eventLoopGroup);
                bindServer(settings);
            }
            super.doStart();
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    private Bootstrap createClientBootstrap(EventLoopGroup eventLoopGroup) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NettyBootstrap.clientChannel());

        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));

        final ByteSizeValue tcpSendBufferSize = TransportSettings.TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = TransportSettings.TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
        }

        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);

        return bootstrap;
    }

    private void createServerBootstrap(Settings settings, EventLoopGroup eventLoopGroup) {
        List<String> transportBindHosts = TransportSettings.BIND_HOST.get(settings);
        List<String> bindHosts = transportBindHosts.isEmpty()
            ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings)
            : transportBindHosts;
        List<String> publishHosts = TransportSettings.PUBLISH_HOST.get(settings);
        String portOrRange = TransportSettings.PORT.get(settings);

        if (logger.isDebugEnabled()) {
            logger.debug(
                "port[{}], bind_host[{}], publish_host[{}], receive_predictor[{}->{}]",
                portOrRange,
                bindHosts,
                publishHosts,
                receivePredictorMin,
                receivePredictorMax);
        }

        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.channel(NettyBootstrap.serverChannel());

        serverBootstrap.childHandler(new ServerChannelInitializer());
        serverBootstrap.handler(new ServerChannelExceptionHandler());

        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));

        long sendBufferBytes = TransportSettings.TCP_SEND_BUFFER_SIZE.get(settings).getBytes();
        if (sendBufferBytes != -1) {
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(sendBufferBytes));
        }
        long receiveBufferBytes = TransportSettings.TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes();
        if (receiveBufferBytes != -1) {
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(receiveBufferBytes));
        }

        serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
        serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        Boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
        serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);
        serverBootstrap.validate();

        this.serverBootstrap = serverBootstrap;
    }

    public static final AttributeKey<CloseableChannel> CHANNEL_KEY = AttributeKey.newInstance("es-channel");
    static final AttributeKey<CloseableChannel> SERVER_CHANNEL_KEY = AttributeKey.newInstance("es-server-channel");

    @Override
    protected ConnectResult initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        Bootstrap bootstrapWithHandler = clientBootstrap.clone();
        bootstrapWithHandler.handler(new ClientChannelInitializer());
        bootstrapWithHandler.remoteAddress(address);
        ChannelFuture connectFuture = bootstrapWithHandler.connect();

        Channel channel = connectFuture.channel();
        if (channel == null) {
            Exceptions.maybeDieOnAnotherThread(connectFuture.cause());
            throw new IOException(connectFuture.cause());
        }
        addClosedExceptionLogger(channel);

        CloseableChannel nettyChannel = new CloseableChannel(channel, false);
        channel.attr(CHANNEL_KEY).set(nettyChannel);

        return new ConnectResult(nettyChannel, connectFuture);
    }

    @Override
    protected CloseableChannel bind(InetSocketAddress address) {
        Channel channel = serverBootstrap.bind(address).syncUninterruptibly().channel();
        CloseableChannel esChannel = new CloseableChannel(channel, true);
        channel.attr(SERVER_CHANNEL_KEY).set(esChannel);
        return esChannel;
    }

    @Override
    @SuppressForbidden(reason = "debug")
    protected void stopInternal() {
        Releasables.close(() -> {
            serverBootstrap = null;
            clientBootstrap = null;
        });
    }

    protected class ClientChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            maybeInjectSSL(ch);
            ch.pipeline().addLast("logging", loggingHandler);
            // using a dot as a prefix means this cannot come from any settings parsed
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(pageCacheRecycler, Netty4Transport.this));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Exceptions.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }

        private void maybeInjectSSL(Channel ch) throws Exception, AssertionError {
            SSLMode sslMode = SslSettings.SSL_TRANSPORT_MODE.get(settings);
            if (sslMode == SSLMode.ON) {
                SslContext sslContext = sslContextProvider.clientContext();
                SslHandler sslHandler = sslContext.newHandler(ch.alloc());
                sslHandler.engine().setUseClientMode(true);
                ch.pipeline().addLast(sslHandler);
            }
        }
    }

    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {

        protected ServerChannelInitializer() {
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            SSLMode sslMode = SslSettings.SSL_TRANSPORT_MODE.get(settings);
            if (sslMode == SSLMode.ON) {
                SslContext sslContext = sslContextProvider.getServerContext(Protocol.TRANSPORT);
                SslHandler sslHandler = sslContext.newHandler(ch.alloc());
                ch.pipeline().addLast(sslHandler);
            }

            if (AuthSettings.AUTH_HOST_BASED_ENABLED_SETTING.get(settings) && sslMode != SSLMode.LEGACY) {
                ch.pipeline().addLast("hba", new HostBasedAuthHandler(authentication));
            }
            addClosedExceptionLogger(ch);
            CloseableChannel nettyTcpChannel = new CloseableChannel(ch, true);
            ch.attr(CHANNEL_KEY).set(nettyTcpChannel);
            serverAcceptedChannel(nettyTcpChannel);
            ch.pipeline().addLast("logging", loggingHandler);
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(pageCacheRecycler, Netty4Transport.this));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Exceptions.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    private void addClosedExceptionLogger(Channel channel) {
        channel.closeFuture().addListener(f -> {
            if (f.isSuccess() == false) {
                logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), f.cause());
            }
        });
    }

    @ChannelHandler.Sharable
    private class ServerChannelExceptionHandler extends ChannelHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            Exceptions.maybeDieOnAnotherThread(cause);
            CloseableChannel serverChannel = ctx.channel().attr(SERVER_CHANNEL_KEY).get();
            if (cause instanceof Error) {
                onServerException(serverChannel, new Exception(cause));
            } else {
                onServerException(serverChannel, (Exception) cause);
            }
        }
    }
}
