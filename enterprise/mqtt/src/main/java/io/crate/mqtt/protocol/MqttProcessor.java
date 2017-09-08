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

package io.crate.mqtt.protocol;

import io.crate.mqtt.netty.NettyUtils;
import io.crate.mqtt.operations.MqttIngestService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Locale;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Provides mechanisms to handle mqtt messages and channel operations (eg. connect and disconnect)
 */
public class MqttProcessor {

    private static final Logger LOGGER = Loggers.getLogger(MqttProcessor.class);

    private final MqttIngestService ingestService;

    public MqttProcessor(MqttIngestService ingestService) {
        this.ingestService = ingestService;
    }

    public void handleConnect(Channel channel, MqttConnectMessage msg) {
        String clientId = msg.payload().clientIdentifier();
        LOGGER.debug("CONNECT for client <{}>", clientId);

        if (!(msg.variableHeader().version() == MqttVersion.MQTT_3_1.protocolLevel()
                || msg.variableHeader().version() == MqttVersion.MQTT_3_1_1.protocolLevel())) {
            sendErrorResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
                    .addListener(f -> LOGGER.warn("CONNECT sent UNACCEPTABLE_PROTOCOL_VERSION"));
            return;
        }

        // We require the clean session header to be true if client id is not provided
        // See MQTT-3.1.3-8 http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
        if (clientId == null || clientId.length() == 0) {
            if (msg.variableHeader().isCleanSession() == false) {
                sendErrorResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED)
                        .addListener(cf -> LOGGER.warn("CONNECT sent IDENTIFIER_REJECTED"));
                return;
            }
            clientId = UUID.randomUUID().toString();
            LOGGER.info("Client connected with server generated identifier: {}", clientId);
        }
        NettyUtils.clientID(channel, clientId);

        int keepAlive = msg.variableHeader().keepAliveTimeSeconds();
        if (keepAlive > 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Client connected with keepAlive of {} s", keepAlive);
            }
            NettyUtils.keepAlive(channel, keepAlive);

            if (channel.pipeline().names().contains("idleStateHandler")) {
                channel.pipeline().remove("idleStateHandler");
            }

            // avoid terminating connections too early
            int allIdleTimeSeconds = Math.round(keepAlive * 1.5f);
            channel.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, allIdleTimeSeconds));
        }

        channel.writeAndFlush(MqttMessageFactory.newConnAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, true))
            .addListener(cf -> LOGGER.info("CONNECT sent CONNECTION_ACCEPTED"));
    }

    private ChannelFuture sendErrorResponse(Channel channel, MqttConnectReturnCode returnCode) {

        return channel.writeAndFlush(MqttMessageFactory.newConnAck(returnCode, false))
            .addListener(ChannelFutureListener.CLOSE);
    }

    public void handlePublish(Channel channel, MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        String clientId = NettyUtils.clientID(channel);
        switch (qos) {
            case AT_LEAST_ONCE:
                SendPubAckCallback ackCallback = new SendPubAckCallback(
                        channel, msg.variableHeader().packetId(), msg.fixedHeader().isDup());
                ingestService.doInsert(clientId, msg, ackCallback);
                break;
            default:
                String message = String.format(Locale.ENGLISH, "QoS %s (%s) is not supported.",
                        qos.value(), qos.name());
                throw new UnsupportedOperationException(message);
        }
    }

    public void handleDisconnect(Channel channel) throws InterruptedException {
        ChannelFuture closeFuture = channel.flush()
            .close();
        if (LOGGER.isDebugEnabled()) {
            closeFuture.addListener(cf -> LOGGER.debug("Client closed connection by sending DISCONNECT"));
        }
    }

    public void handlePingReq(Channel channel) {
        channel.writeAndFlush(MqttMessageFactory.newPingResponse(MqttQoS.AT_LEAST_ONCE))
            .addListener(cf -> LOGGER.info("PINGRESP sent"));
    }

    private static class SendPubAckCallback implements BiConsumer<Object, Throwable> {

        private final Channel channel;
        private final Integer packetId;
        private final boolean isDupFlag;

        private SendPubAckCallback(Channel channel, Integer packetId, boolean isDupFlag) {
            this.channel = channel;
            this.packetId = packetId;
            this.isDupFlag = isDupFlag;
        }

        @Override
        public void accept(Object result, Throwable t) {
            if (t == null) {
                sendPubAck(channel, packetId, isDupFlag);
            } else {
                LOGGER.info("Failed to insert MQTT message into CrateDB: {}", t.getMessage());
            }
        }

        private static void sendPubAck(Channel channel, int packetId, boolean isDupFlag) {
            ChannelFuture channelFuture =
                channel.writeAndFlush(MqttMessageFactory.newPubAckMessage(MqttQoS.AT_LEAST_ONCE, isDupFlag, packetId));
            if (LOGGER.isTraceEnabled()) {
                channelFuture.addListener(cf -> LOGGER.trace("PUBACK sent"));
            }
        }
    }
}
