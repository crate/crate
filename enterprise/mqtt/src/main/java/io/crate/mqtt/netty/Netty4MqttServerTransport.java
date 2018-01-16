/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.mqtt.netty;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import io.crate.action.sql.SQLOperations;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.mqtt.operations.MqttIngestService;
import io.crate.mqtt.protocol.MqttProcessor;
import io.crate.netty.CrateChannelBootstrapFactory;
import io.crate.auth.user.UserManager;
import io.crate.protocols.postgres.BindPostgresException;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.settings.CrateSetting;
import io.crate.settings.SharedSettings;
import io.crate.types.DataTypes;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.transport.BindTransportException;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@Singleton
public class Netty4MqttServerTransport extends AbstractLifecycleComponent {

    public static final CrateSetting<Boolean> MQTT_ENABLED_SETTING = CrateSetting.of(
        Setting.boolSetting("ingestion.mqtt.enabled", false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<Boolean> SSL_MQTT_ENABLED = CrateSetting.of(
        Setting.boolSetting("ssl.ingestion.mqtt.enabled", false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<String> MQTT_PORT_SETTING = CrateSetting.of(new Setting<>(
        "ingestion.mqtt.port", "1883",
        Function.identity(), Setting.Property.NodeScope), DataTypes.STRING);

    public static final CrateSetting<TimeValue> MQTT_TIMEOUT_SETTING = CrateSetting.of(Setting.timeSetting(
        "ingestion.mqtt.timeout", TimeValue.timeValueSeconds(10L), TimeValue.timeValueSeconds(1L),
        Setting.Property.NodeScope), DataTypes.STRING);

    static boolean isMQTTSslEnabled(Settings settings) {
        return SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings) &&
               SSL_MQTT_ENABLED.setting().get(settings);
    }

    private final NetworkService networkService;
    private final String port;
    private final Logger logger;
    private final TimeValue defaultIdleTimeout;
    private final boolean isEnterprise;
    private final boolean isEnabled;
    private final MqttMessageLogger mqttMessageLogger;
    private final List<Channel> serverChannels = new ArrayList<>();
    private final List<InetSocketTransportAddress> boundAddresses = new ArrayList<>();
    private final SslContextProvider sslContextProvider;
    private ServerBootstrap serverBootstrap;
    private final MqttIngestService mqttIngestService;

    private BoundTransportAddress boundAddress;

    @Inject
    public Netty4MqttServerTransport(Settings settings,
                                     NetworkService networkService,
                                     Functions functions,
                                     SQLOperations sqlOperations,
                                     UserManager userManager,
                                     IngestionService ingestionService,
                                     SslContextProvider sslContextProvider) {
        super(settings);
        this.networkService = networkService;
        logger = Loggers.getLogger("mqtt", settings);
        isEnterprise = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        isEnabled = MQTT_ENABLED_SETTING.setting().get(settings);
        port = MQTT_PORT_SETTING.setting().get(settings);
        defaultIdleTimeout = MQTT_TIMEOUT_SETTING.setting().get(settings);
        mqttMessageLogger = new MqttMessageLogger(settings);
        mqttIngestService = new MqttIngestService(functions, sqlOperations, userManager, ingestionService);
        this.sslContextProvider = sslContextProvider;
    }

    @Override
    protected void doStart() {
        if (isEnterprise == false || isEnabled == false) {
            return;
        }

        mqttIngestService.initialize();
        serverBootstrap = CrateChannelBootstrapFactory.newChannelBootstrap("mqtt", settings);
        serverBootstrap.childHandler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                final MqttProcessor processor = new MqttProcessor(mqttIngestService);
                final MqttNettyHandler handler = new MqttNettyHandler(processor);
                final MqttNettyIdleTimeoutHandler timeoutHandler = new MqttNettyIdleTimeoutHandler();
                final IdleStateHandler defaultIdleHandler = new IdleStateHandler(0L, 0L,
                    defaultIdleTimeout.seconds(), TimeUnit.SECONDS);

                pipeline.addFirst("idleStateHandler", defaultIdleHandler)
                    .addAfter("idleStateHandler", "idleEventHandler", timeoutHandler)
                    .addLast("decoder", new MqttDecoder())
                    .addLast("encoder", MqttEncoder.INSTANCE)
                    .addLast("messageLogger", mqttMessageLogger)
                    .addLast("handler", handler);

                if (isMQTTSslEnabled(settings)) {
                    SslHandler sslHandler = sslContextProvider.get().newHandler(pipeline.channel().alloc());
                    pipeline.addFirst(sslHandler);
                }
            }
        });
        serverBootstrap.validate();

        boolean success = false;
        try {
            boundAddress = resolveBindAddress();
            logger.info("{}", boundAddress);
            success = true;
        } finally {
            if (!success) {
                doStop(); // stop boss/worker threads to avoid leaks
            }
        }
    }

    public BoundTransportAddress getBoundAddress() {
        return boundAddress;
    }

    private static int resolvePublishPort(List<InetSocketTransportAddress> boundAddresses, InetAddress publishInetAddress) {
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

        throw new BindHttpException("Failed to auto-resolve mqtt publish port, multiple bound addresses " +
                                    boundAddresses +
                                    " with distinct ports and none of them matched the publish address (" +
                                    publishInetAddress + "). " +
                                    "Please specify a unique port by setting " + MQTT_PORT_SETTING.getKey());
    }

    private TransportAddress bindAddress(final InetAddress hostAddress) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                synchronized (serverChannels) {
                    ChannelFuture future = serverBootstrap.bind(new InetSocketAddress(hostAddress, portNumber)).sync();
                    serverChannels.add(future.channel());
                    boundSocket.set((InetSocketAddress) future.channel().localAddress());
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindHttpException("Failed to bind to [" + portsRange.getPortRangeString() + "]",
                lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound mqtt to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new InetSocketTransportAddress(boundSocket.get());
    }

    private BoundTransportAddress resolveBindAddress() {
        // Bind and start to accept incoming connections.
        try {
            InetAddress[] hostAddresses = networkService.resolveBindHostAddresses(null);
            for (InetAddress address : hostAddresses) {
                if (address instanceof Inet4Address) {
                    boundAddresses.add((InetSocketTransportAddress) bindAddress(address));
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
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[boundAddresses.size()]),
            new InetSocketTransportAddress(publishAddress));
    }

    @Override
    protected void doStop() {
        for (Channel channel : serverChannels) {
            channel.close().awaitUninterruptibly();
        }
        serverChannels.clear();
        if (serverBootstrap != null) {
            ServerBootstrapConfig config = serverBootstrap.config();
            config.group().shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            config.childGroup().shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            serverBootstrap = null;
        }
    }

    @Override
    protected void doClose() {
    }
}
