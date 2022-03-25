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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.OutboundHandler;
import org.elasticsearch.transport.RemoteClusterAwareRequest;
import org.elasticsearch.transport.RemoteConnectionManager.ProxyConnection;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4MessageChannelHandler;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.Netty4Utils;

import io.crate.common.collections.BorrowedItem;
import io.crate.exceptions.Exceptions;
import io.crate.netty.NettyBootstrap;
import io.crate.user.User;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * A client that uses the PostgreSQL wire protocol to initiate a connection,
 * but then switches over to use the transport protocol
 **/
public class PgClient extends AbstractClient {

    private static final Logger LOGGER = LogManager.getLogger(PgClient.class);

    final NettyBootstrap nettyBootstrap;
    final Netty4Transport transport;
    final PageCacheRecycler pageCacheRecycler;
    final DiscoveryNode host;
    final TransportService transportService;
    final AtomicBoolean isClosing = new AtomicBoolean(false);
    final String username;
    final String password;
    final ConnectionProfile profile;

    private CompletableFuture<Transport.Connection> connectionFuture;
    private BorrowedItem<EventLoopGroup> eventLoopGroup;
    private Bootstrap bootstrap;
    private Channel channel;

    public PgClient(Settings nodeSettings,
                    TransportService transportService,
                    NettyBootstrap nettyBootstrap,
                    Netty4Transport transport,
                    PageCacheRecycler pageCacheRecycler,
                    DiscoveryNode host,
                    @Nullable String username,
                    @Nullable String password) {
        super(nodeSettings, transport.getThreadPool());
        this.transportService = transportService;
        this.nettyBootstrap = nettyBootstrap;
        this.transport = transport;
        this.pageCacheRecycler = pageCacheRecycler;
        this.host = host;
        this.username = username;
        this.password = password;
        this.profile = new ConnectionProfile.Builder()
            .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setPingInterval(TransportSettings.PING_SCHEDULE.get(settings))
            .setCompressionEnabled(TransportSettings.TRANSPORT_COMPRESS.get(settings))
            .addConnections(1, TransportRequestOptions.Type.BULK)
            .addConnections(1, TransportRequestOptions.Type.PING)
            .addConnections(1, TransportRequestOptions.Type.STATE)
            .addConnections(1, TransportRequestOptions.Type.RECOVERY)
            .addConnections(1, TransportRequestOptions.Type.REG)
            .build();
    }

    public CompletableFuture<Transport.Connection> ensureConnected() {
        if (isClosing.get()) {
            return CompletableFuture.failedFuture(new AlreadyClosedException("PgClient is closed"));
        }
        CompletableFuture<Transport.Connection> future;
        synchronized (this) {
            if (connectionFuture == null) {
                connectionFuture = new CompletableFuture<>();
                future = connectionFuture;
                // fall-through to connect
            } else {
                if (connectionFuture.isDone()) {
                    Connection connection = connectionFuture.join();
                    if (connection.isClosed() || connectionFuture.isCompletedExceptionally()) {
                        connectionFuture = new CompletableFuture<>();
                        future = connectionFuture;
                        // fall-through to connect
                    } else {
                        return connectionFuture;
                    }
                } else {
                    return connectionFuture;
                }
            }
        }
        try {
            return connect(future);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }
    }

    public CompletableFuture<Transport.Connection> connect(CompletableFuture<Connection> future) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Connecting to {}", host);
        }
        bootstrap = new Bootstrap();
        eventLoopGroup = nettyBootstrap.getSharedEventLoopGroup(settings);
        bootstrap.group(eventLoopGroup.item());
        bootstrap.channel(NettyBootstrap.clientChannel());
        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));

        bootstrap.handler(new ClientChannelInitializer(
            profile,
            host,
            transport,
            pageCacheRecycler,
            password,
            future
        ));
        bootstrap.remoteAddress(host.getAddress().address());
        ChannelFuture connectFuture = bootstrap.connect();
        channel = connectFuture.channel();
        Netty4TcpChannel nettyChannel = new Netty4TcpChannel(
            channel,
            false,
            "default",
            connectFuture
        );
        channel.attr(Netty4Transport.CHANNEL_KEY).set(nettyChannel);
        connectFuture.addListener(f -> {
            if (f.isSuccess()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Connection to {} via pgwire established. Sending startup to switchover to transport protocol", host);
                }

                ByteBuf buffer = channel.alloc().buffer();
                String user = username == null ? User.CRATE_USER.name() : username;
                Map<String, String> properties = Map.of("user", user, "CrateDBTransport", "true");
                ClientMessages.sendStartupMessage(buffer, "doc", properties);
                channel.writeAndFlush(buffer);
            } else {
                Throwable cause = f.cause();
                future.completeExceptionally(cause);
            }
        });

        return future;
    }

    @Override
    public void close() {
        if (isClosing.compareAndSet(false, true)) {
            if (bootstrap != null) {
                bootstrap = null;
            }
            if (eventLoopGroup != null) {
                eventLoopGroup.close();
                eventLoopGroup = null;
            }
            if (channel != null) {
                try {
                    Netty4Utils.closeChannels(List.of(channel));
                } catch (IOException ignored) {
                }
                channel = null;
            }
            var future = connectionFuture;
            connectionFuture = null;
            if (future != null) {
                if (!future.isDone()) {
                    future.completeExceptionally(new AlreadyClosedException("PgClient is closed"));
                } else if (!future.isCompletedExceptionally()) {
                    Connection connection = future.join();
                    connection.close();
                }
            }
        }
    }

    static class ClientChannelInitializer extends ChannelInitializer<Channel> {

        private final DiscoveryNode node;
        private final Netty4Transport transport;
        private final CompletableFuture<Transport.Connection> result;
        private final PageCacheRecycler pageCacheRecycler;
        private final ConnectionProfile profile;
        private final String password;

        public ClientChannelInitializer(ConnectionProfile profile,
                                        DiscoveryNode node,
                                        Netty4Transport transport,
                                        PageCacheRecycler pageCacheRecycler,
                                        String password,
                                        CompletableFuture<Transport.Connection> result) {
            this.profile = profile;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.password = password;
            this.result = result;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", new Decoder());

            Handler handler = new Handler(
                profile,
                node,
                transport,
                pageCacheRecycler,
                password,
                result
            );
            ch.pipeline().addLast("dispatcher", handler);
        }
    }

    static class Handler extends SimpleChannelInboundHandler<ByteBuf> {

        private final CompletableFuture<Transport.Connection> result;
        private final PageCacheRecycler pageCacheRecycler;
        private final DiscoveryNode node;
        private final Netty4Transport transport;
        private final ConnectionProfile profile;
        private final String password;

        public Handler(ConnectionProfile profile,
                       DiscoveryNode node,
                       Netty4Transport transport,
                       PageCacheRecycler pageCacheRecycler,
                       @Nullable String password,
                       CompletableFuture<Transport.Connection> result) {
            this.profile = profile;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.password = password;
            this.result = result;
        }

        @Override
        public boolean acceptInboundMessage(Object msg) throws Exception {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte msgType = msg.readByte();
            msg.readInt(); // consume msgLength

            switch (msgType) {
                case 'R' -> handleAuth(ctx.channel(), msg);
                case 'S' -> handleParameterStatus(msg);
                case 'Z' -> handleReadyForQuery(ctx.channel(), msg);
                case 'E' -> handleErrorResponse(msg);
                default -> throw new IllegalStateException("Unexpected message type: " + msgType);
            }
        }

        private void handleErrorResponse(ByteBuf msg) {
            ArrayList<String> errorMessages = new ArrayList<>();
            while (msg.readByte() != 0) {
                String error = PostgresWireProtocol.readCString(msg);
                errorMessages.add(error);
            }
            result.completeExceptionally(new IllegalStateException("Error response: " + String.join(", ", errorMessages)));
        }

        private void handleReadyForQuery(Channel channel, ByteBuf msg) {
            msg.readByte(); // consume transaction status

            upgradeToTransportProtocol(channel);
        }

        private void upgradeToTransportProtocol(Channel channel) {
            channel.pipeline().remove("decoder");
            channel.pipeline().remove("dispatcher");

            // The Netty4MessageChannelHandler retrieves the `Channel` to use via `ctx.channel().attr(Netty4Transport.CHANNEL_KEY)`
            // We set that property earlier in `connect()` to the pg-client channel.
            // This lets the tcp-transport communicate via the channel established here in the PgClient.

            var handler = new Netty4MessageChannelHandler(pageCacheRecycler, transport);
            channel.pipeline().addLast("dispatcher", handler);
            Netty4TcpChannel tcpChannel = channel.attr(Netty4Transport.CHANNEL_KEY).get();

            ActionListener<Version> onHandshakeResponse = ActionListener.wrap(
                version -> {
                    var connection = new TunneledConnection(
                        transport.outboundHandler(),
                        node,
                        tcpChannel,
                        profile,
                        version
                    );
                    long relativeMillisTime = this.transport.getThreadPool().relativeTimeInMillis();
                    tcpChannel.getChannelStats().markAccessed(relativeMillisTime);
                    tcpChannel.addCloseListener(ActionListener.wrap(connection::close));
                    result.complete(connection);
                },
                e -> {
                    var error = e instanceof ConnectTransportException ? e : new ConnectTransportException(node, "general node connection failure", e);
                    try {
                        CloseableChannel.closeChannels(List.of(tcpChannel), false);
                    } catch (Exception ex) {
                        error.addSuppressed(ex);
                    } finally {
                        result.completeExceptionally(error);
                    }
                }
            );
            transport.executeHandshake(node, tcpChannel, profile, onHandshakeResponse);
        }

        private void handleParameterStatus(ByteBuf msg) {
            PostgresWireProtocol.readCString(msg); // consume name
            PostgresWireProtocol.readCString(msg); // consume value
        }

        private void handleAuth(Channel channel, ByteBuf msg) {
            AuthType authType = AuthType.of(msg.readInt());
            switch (authType) {
                case OK:
                    break;

                case CLEARTEXT_PASSWORD:
                    ByteBuf buffer = channel.alloc().buffer();
                    channel.writeAndFlush(ClientMessages.writePasswordMessage(buffer, password));
                    break;

                default:
                    throw new UnsupportedOperationException(authType + " is not supported");
            }
        }
    }

    enum AuthType {
        OK,
        KERBEROS,
        CLEARTEXT_PASSWORD,
        MD5_PASSWORD;

        public static AuthType of(int type) {
            return switch (type) {
                case 0 -> OK;
                case 2 -> KERBEROS;
                case 3 -> CLEARTEXT_PASSWORD;
                case 5 -> MD5_PASSWORD;
                default -> throw new IllegalArgumentException("Unknown auth type: " + type);
            };
        }
    }


    static class Decoder extends LengthFieldBasedFrameDecoder {

        // PostgreSQL wire protocol message format:
        // | Message Type (Byte1) | Length including self (Int32) | Body (depending on type) |
        private static final int LENGTH_FIELD_OFFSET = 1;
        private static final int LENGTH_FIELD_LENGTH = 4;
        private static final int LENGTH_ADJUSTMENT = -4;

        // keep the header
        private static final int INITIAL_BYTES_TO_STRIP = 0;

        public Decoder() {
            super(
                Integer.MAX_VALUE,
                LENGTH_FIELD_OFFSET,
                LENGTH_FIELD_LENGTH,
                LENGTH_ADJUSTMENT,
                INITIAL_BYTES_TO_STRIP
            );
        }
    }


    public static class TunneledConnection extends CloseableConnection {

        private final DiscoveryNode node;
        private final Netty4TcpChannel channel;
        private final ConnectionProfile connectionProfile;
        private final Version version;
        private final OutboundHandler outboundHandler;
        private final AtomicBoolean isClosing = new AtomicBoolean(false);

        public TunneledConnection(OutboundHandler outboundHandler,
                                  DiscoveryNode node,
                                  Netty4TcpChannel channel,
                                  ConnectionProfile connectionProfile,
                                  Version version) {
            this.outboundHandler = outboundHandler;
            this.node = node;
            this.channel = channel;
            this.connectionProfile = connectionProfile;
            this.version = version;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public void sendRequest(long requestId,
                                String action,
                                TransportRequest request,
                                TransportRequestOptions options) throws IOException, TransportException {
            if (isClosing.get()) {
                throw new NodeNotConnectedException(node, "connection already closed");
            }
            outboundHandler.sendRequest(
                node,
                channel,
                requestId,
                action,
                request,
                options,
                version,
                connectionProfile.getCompressionEnabled(),
                false // isHandshake
            );
        }


        @Override
        public void close() {
            if (isClosing.compareAndSet(false, true)) {
                try {
                    CloseableChannel.closeChannels(List.of(channel), false);
                } finally {
                    super.close();
                }
            }
        }
    }

    @Override
    protected <Request extends TransportRequest, Response extends TransportResponse> void doExecute(ActionType<Response> action,
                                                                                                    Request request,
                                                                                                    ActionListener<Response> listener) {
        ensureConnected().whenComplete((connection, e) -> {
            if (e != null) {
                listener.onFailure(Exceptions.toRuntimeException(e));
            } else {
                if (request instanceof RemoteClusterAwareRequest remoteClusterAware) {
                    DiscoveryNode targetNode = remoteClusterAware.getPreferredTargetNode();
                    transportService.sendRequest(
                        new ProxyConnection(connection, targetNode),
                        action.name(),
                        request,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(listener, action.getResponseReader())
                    );
                } else {
                    transportService.sendRequest(
                        connection,
                        action.name(),
                        request,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(listener, action.getResponseReader())
                    );
                }
            }
        });
    }
}
