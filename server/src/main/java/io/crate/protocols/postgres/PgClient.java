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
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.OutboundHandler;
import org.elasticsearch.transport.ProxyConnection;
import org.elasticsearch.transport.RemoteClusterAwareRequest;
import org.elasticsearch.transport.RemoteConnectionParser;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4MessageChannelHandler;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.Netty4Utils;

import io.crate.action.FutureActionListener;
import io.crate.netty.NettyBootstrap;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.replication.logical.metadata.ConnectionInfo;
import io.crate.replication.logical.metadata.ConnectionInfo.SSLMode;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

/**
 * A client that uses the PostgreSQL wire protocol to initiate a connection,
 * but then switches over to use the transport protocol
 **/
public class PgClient extends AbstractClient {

    private static final Logger LOGGER = LogManager.getLogger(PgClient.class);

    final String name;
    final NettyBootstrap nettyBootstrap;
    final Netty4Transport transport;
    final PageCacheRecycler pageCacheRecycler;
    final DiscoveryNode host;
    final TransportService transportService;
    final AtomicBoolean isClosing = new AtomicBoolean(false);
    final ConnectionInfo connectionInfo;
    final ConnectionProfile profile;
    final SslContextProvider sslContextProvider;

    private CompletableFuture<Transport.Connection> connectionFuture;

    public PgClient(String name,
                    Settings nodeSettings,
                    TransportService transportService,
                    NettyBootstrap nettyBootstrap,
                    Netty4Transport transport,
                    SslContextProvider sslContextProvider,
                    PageCacheRecycler pageCacheRecycler,
                    ConnectionInfo connectionInfo) {
        super(nodeSettings, transport.getThreadPool());
        this.name = name;
        this.transportService = transportService;
        this.nettyBootstrap = nettyBootstrap;
        this.transport = transport;
        this.sslContextProvider = sslContextProvider;
        this.pageCacheRecycler = pageCacheRecycler;
        this.host = toDiscoveryNode(connectionInfo.hosts());
        this.connectionInfo = connectionInfo;
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

    private DiscoveryNode toDiscoveryNode(List<String> hosts) {
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException("No hosts configured for pg tunnel " + name);
        }
        String host = hosts.get(0);
        var transportAddress = new TransportAddress(RemoteConnectionParser.parseConfiguredAddress(host));
        return new DiscoveryNode(
            "pg_tunnel_to=" + name + "#" + transportAddress.toString(),
            transportAddress,
            Version.CURRENT.minimumCompatibilityVersion()
        );
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
                if (connectionFuture.isCompletedExceptionally() || (connectionFuture.isDone() && connectionFuture.join().isClosed())) {
                    connectionFuture = new CompletableFuture<>();
                    future = connectionFuture;
                    // fall-through to connect
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
        var bootstrap = new Bootstrap();
        var eventLoopGroup = nettyBootstrap.getSharedEventLoopGroup();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NettyBootstrap.clientChannel());
        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
        bootstrap.handler(new ClientChannelInitializer(
            profile,
            host,
            transport,
            pageCacheRecycler,
            sslContextProvider,
            connectionInfo,
            future
        ));
        bootstrap.remoteAddress(host.getAddress().address());
        ChannelFuture connectFuture = bootstrap.connect();
        var channel = connectFuture.channel();
        future.exceptionally(err -> {
            try {
                Netty4Utils.closeChannels(List.of(channel));
            } catch (IOException ignored) {
            }
            return null;
        });
        CloseableChannel nettyChannel = new CloseableChannel(
            channel,
            false
        );
        channel.attr(Netty4Transport.CHANNEL_KEY).set(nettyChannel);
        connectFuture.addListener(f -> {
            if (f.isSuccess()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Connection to {} via pgwire established. Sending startup to switchover to transport protocol", host);
                }

                ByteBuf buffer = channel.alloc().buffer();
                SSLMode sslMode = connectionInfo.sslMode();
                if (sslMode == SSLMode.REQUIRE || sslMode == SSLMode.PREFER) {
                    ClientMessages.writeSSLReqMessage(buffer);
                } else {
                    String user = connectionInfo.user();
                    Map<String, String> properties = Map.of("user", user, "CrateDBTransport", "true");
                    ClientMessages.sendStartupMessage(buffer, "doc", properties);
                }
                channel.writeAndFlush(buffer);
            } else {
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    @Override
    public void close() {
        if (isClosing.compareAndSet(false, true)) {
            CompletableFuture<Connection> future;
            synchronized (this) {
                future = connectionFuture;
                connectionFuture = null;
            }
            if (future != null) {
                future.thenAccept(conn -> conn.close());
            }
        }
    }

    static class ClientChannelInitializer extends ChannelInitializer<Channel> {

        private final DiscoveryNode node;
        private final Netty4Transport transport;
        private final CompletableFuture<Transport.Connection> result;
        private final PageCacheRecycler pageCacheRecycler;
        private final ConnectionProfile profile;
        private final ConnectionInfo connectionInfo;
        private final SslContextProvider sslContextProvider;

        public ClientChannelInitializer(ConnectionProfile profile,
                                        DiscoveryNode node,
                                        Netty4Transport transport,
                                        PageCacheRecycler pageCacheRecycler,
                                        SslContextProvider sslContextProvider,
                                        ConnectionInfo connectionInfo,
                                        CompletableFuture<Transport.Connection> result) {
            this.profile = profile;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.sslContextProvider = sslContextProvider;
            this.connectionInfo = connectionInfo;
            this.result = result;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            SSLMode sslMode = connectionInfo.sslMode();
            SslContext sslCtx = null;
            if (sslMode == SSLMode.REQUIRE || sslMode == SSLMode.PREFER) {
                try {
                    sslCtx = sslContextProvider.clientContext();
                } catch (Exception e) {
                    sslCtx = SslContextBuilder.forClient().build();
                }
            }
            pipeline.addLast("decoder", new Decoder(connectionInfo.user(), sslMode, sslCtx));
            Handler handler = new Handler(
                profile,
                node,
                transport,
                pageCacheRecycler,
                connectionInfo,
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
        private final ConnectionInfo connectionInfo;

        public Handler(ConnectionProfile profile,
                       DiscoveryNode node,
                       Netty4Transport transport,
                       PageCacheRecycler pageCacheRecycler,
                       ConnectionInfo connectionInfo,
                       CompletableFuture<Transport.Connection> result) {
            this.profile = profile;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.connectionInfo = connectionInfo;
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
                case 'K' -> handleKeyData(msg);
                default -> result.completeExceptionally(
                    new IllegalStateException("Unexpected message type: " + msgType));
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

        private void handleKeyData(ByteBuf msg) {
            // do nothing until there is a use for KeyData
            KeyData.of(msg);
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
            CloseableChannel tcpChannel = channel.attr(Netty4Transport.CHANNEL_KEY).get();

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
                    tcpChannel.markAccessed(relativeMillisTime);
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Server sent authentication request type={}", authType);
            }
            switch (authType) {
                case OK:
                    break;

                case CLEARTEXT_PASSWORD:
                    ByteBuf buffer = channel.alloc().buffer();
                    ClientMessages.writePasswordMessage(buffer, connectionInfo.password());
                    channel.writeAndFlush(buffer);
                    break;

                default:
                    throw new UnsupportedOperationException(authType + " is not supported");
            }
        }
    }

    enum AuthType {
        OK,
        CLEARTEXT_PASSWORD;

        public static AuthType of(int type) {
            return switch (type) {
                case 0 -> OK;
                case 3 -> CLEARTEXT_PASSWORD;
                default -> throw new IllegalArgumentException("Unknown auth type: " + type);
            };
        }
    }


    /**
     * Ensures the next handler in the netty pipeline receives complete PostgreSQL messages.
     * Also takes care of the SSL negotiation (incl. sending a startup message) if the sslContext is present.
     **/
    static class Decoder extends ByteToMessageDecoder {

        /**
         * Minimum length of a message.
         *
         * <ul>
         * <li>
         *  <code>1 byte: message type</code>
         * </li>
         *
         * <li>
         *  <code>4 byte: message length</code>
         * </li>
         **/
        private static final int HEADER_LENGTH = 5;

        private final SslContext sslContext;
        private final String user;
        private final SSLMode sslMode;

        private boolean expectSSLResponse;


        public Decoder(String user, SSLMode sslMode, SslContext sslContext) {
            this.user = user;
            this.sslMode = sslMode;
            this.sslContext = sslContext;
            this.expectSSLResponse = sslMode == SSLMode.REQUIRE || sslMode == SSLMode.PREFER;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            ByteBuf buf = decode(ctx, in);
            if (buf != null) {
                out.add(buf);
            }
        }

        private ByteBuf decode(ChannelHandlerContext ctx, ByteBuf in) {
            if (expectSSLResponse) {
                if (in.readableBytes() < 1) {
                    return null;
                }
                expectSSLResponse = false;
                byte sslResponse = in.readByte();
                if (sslResponse == 'S') {
                    injectSSLHandler(ctx);
                    sendStartup(ctx);
                    // fall-through to handle the rest of the message
                } else if (sslResponse == 'N') {
                    if (sslMode == SSLMode.REQUIRE) {
                        throw new IllegalStateException("SSL required but not supported");
                    } else {
                        sendStartup(ctx);
                        // fall-through to handle the rest of the message
                    }
                } else {
                    throw new IllegalStateException("Unexpected SSL response: " + sslResponse);
                }
            }

            if (in.readableBytes() < HEADER_LENGTH) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("PgDecoder needs more bytes readableBytes={}", in.readableBytes());
                }
                return null;
            }

            in.markReaderIndex();
            byte msgType = in.readByte();
            int msgLength = in.readInt() - 4; // exclude length of msgLength itself

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Decoding message type={} length={} readableBytes={}",
                    (char) msgType,
                    msgLength,
                    in.readableBytes()
                );
            }

            if (in.readableBytes() < msgLength) {
                in.resetReaderIndex();
                return null;
            }

            // Allow handler to read msgType and msgLength again
            in.resetReaderIndex();
            return in.readBytes(HEADER_LENGTH + msgLength);
        }

        private void injectSSLHandler(ChannelHandlerContext ctx) {
            ChannelPipeline pipeline = ctx.pipeline();
            SslHandler sslHandler = sslContext.newHandler(ctx.alloc());
            pipeline.addFirst(sslHandler);
        }

        private void sendStartup(ChannelHandlerContext ctx) {
            ByteBuf buffer = ctx.alloc().buffer();
            Map<String, String> properties = Map.of("user", user, "CrateDBTransport", "true");
            ClientMessages.sendStartupMessage(buffer, "doc", properties);
            ChannelFuture flushStartup = ctx.writeAndFlush(buffer);
            if (LOGGER.isWarnEnabled()) {
                flushStartup.addListener(f -> {
                    if (!f.isSuccess()) {
                        LOGGER.warn("Client failed to send startup message", f.cause());
                    }
                });
            }
        }
    }


    public static class TunneledConnection extends CloseableConnection {

        private final DiscoveryNode node;
        private final CloseableChannel channel;
        private final ConnectionProfile connectionProfile;
        private final Version version;
        private final OutboundHandler outboundHandler;
        private final AtomicBoolean isClosing = new AtomicBoolean(false);

        public TunneledConnection(OutboundHandler outboundHandler,
                                  DiscoveryNode node,
                                  CloseableChannel channel,
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

    public <Request extends TransportRequest, Response extends TransportResponse> CompletableFuture<Response> execute(ActionType<Response> action, Request request) {
        return ensureConnected().thenCompose(connection -> {
            FutureActionListener<Response, Response> future = FutureActionListener.newInstance();
            if (request instanceof RemoteClusterAwareRequest remoteClusterAware) {
                DiscoveryNode targetNode = remoteClusterAware.getPreferredTargetNode();
                transportService.sendRequest(
                    new ProxyConnection(connection, targetNode),
                    action.name(),
                    request,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(future, action.getResponseReader())
                );
            } else {
                transportService.sendRequest(
                    connection,
                    action.name(),
                    request,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(future, action.getResponseReader())
                );
            }
            return future;
        });
    }
}
