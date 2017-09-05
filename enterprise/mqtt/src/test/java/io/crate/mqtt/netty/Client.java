/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.crate.mqtt.netty;

import io.crate.mqtt.protocol.MqttMessageBuilders;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * Class used just to send and receive MQTT messages without any protocol login
 * in action, just use the encoder/decoder part.
 *
 * @author andrea
 */
public class Client implements AutoCloseable {

    private static final Logger LOGGER = Loggers.getLogger(Client.class);

    private final String host;
    private final int port;
    private EventLoopGroup workerGroup;
    private Channel channel;
    private String clientId;
    private ClientNettyMQTTHandler handler;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
        clientId(UUID.randomUUID().toString());
    }

    public void connect() {
        LOGGER.debug("[mqtt-client] connect");
        handler = new ClientNettyMQTTHandler();
        workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("decoder", new MqttDecoder());
                    pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    pipeline.addLast("handler", handler);
                }
            });

            // Start the client.
            channel = b.connect(host, port).sync().channel();
        } catch (Exception ex) {
            LOGGER.error("[mqtt-client] Error in client setup: " + ex.getMessage());
            ex.printStackTrace();
            workerGroup.shutdownGracefully();
        }
    }

    public void clientId(String clientId) {
        this.clientId = clientId;
    }

    public String clientId() {
        return clientId;
    }

    public void sendMessage(MqttMessage msg) {
        LOGGER.debug("[mqtt-client] ==> send message");
        handler.initCallback();
        channel.writeAndFlush(msg);
    }

    public MqttMessage lastReceivedMessage() throws Exception {
        return lastReceivedMessage(1_000L);
    }

    public MqttMessage lastReceivedMessage(long timeout) throws Exception {
        try {
            LOGGER.debug("[mqtt-client] <== receive message");
            return handler.lastMessage(timeout);
        } catch (TimeoutException e) {
            throw new NoMessageReceivedException(timeout);
        }
    }

    @Override
    public void close() throws InterruptedException {
        // Wait until the connection is closed.
        channel.closeFuture().sync();
        if (workerGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        workerGroup.shutdownGracefully();
        LOGGER.debug("[mqtt-client] closed");
    }

    public void disconnect() {
        LOGGER.debug("[mqtt-client] disconnect");
        sendMessage(MqttMessageBuilders.disconnect()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .build());
    }

    static class NoMessageReceivedException extends ExecutionException {
        NoMessageReceivedException(long timeout) {
            super("MQTT client did not receive message in time (" + String.valueOf(timeout) + "ms).");
        }
    }
}
