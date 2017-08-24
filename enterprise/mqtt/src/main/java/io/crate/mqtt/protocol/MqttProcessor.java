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

import io.crate.mqtt.operations.MqttIngestService;
import io.moquette.server.netty.NettyUtils;
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
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.Locale;
import java.util.UUID;
import java.util.function.BiConsumer;


public class MqttProcessor {

    private static final Logger LOGGER = Loggers.getLogger(MqttProcessor.class);
    private final MqttIngestService ingestService;

    public MqttProcessor(MqttIngestService ingestService) {
        this.ingestService = ingestService;
    }

    public void handleConnect(Channel channel, MqttConnectMessage msg) {
        String clientId = msg.payload().clientIdentifier();
        LOGGER.info("CONNECT for client <{}>", clientId);

        if (!(msg.variableHeader().version() == MqttVersion.MQTT_3_1.protocolLevel()
                || msg.variableHeader().version() == MqttVersion.MQTT_3_1_1.protocolLevel())) {
            sendErrorResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION)
                    .addListener(f -> LOGGER.warn("CONNECT sent UNACCEPTABLE_PROTOCOL_VERSION"));
            return;
        }

        /*
         If you don’t need a state to be hold by the broker,
         in MQTT 3.1.1 (current standard) it is also possible to send an empty ClientId,
         which results in a connection without any state.
         A condition is that clean session is true, otherwise the connection will be rejected.
         http://www.hivemq.com/blog/mqtt-essentials-part-3-client-broker-connection-establishment
         */
        if (clientId == null || clientId.length() == 0) {
            if (!msg.variableHeader().isCleanSession()) {
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
            channel.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, Math.round(keepAlive * 1.5f)));
        }

        channel.writeAndFlush(MqttMessageBuilders.connAck()
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .sessionPresent(true)
                .build())
                .addListener(cf -> LOGGER.info("CONNECT sent CONNECTION_ACCEPTED"));
    }

    private ChannelFuture sendErrorResponse(Channel channel, MqttConnectReturnCode returnCode) {

        return channel.writeAndFlush(MqttMessageBuilders.connAck()
                .returnCode(returnCode)
                .sessionPresent(false)
                .build())
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
        channel.flush()
                .close()
                .addListener(cf -> LOGGER.debug("Client closed connection by sending DISCONNECT"));
        ingestService.close();
    }

    public void handlePingReq(Channel channel) {
        channel.writeAndFlush(MqttMessageBuilders.pingResp()
            .qos(MqttQoS.AT_LEAST_ONCE)
            .build())
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
            // If the DUP flag is set by the client a version conflict is ignored and a PubAck is returned.
            if (t == null || (t.getClass() == VersionConflictEngineException.class && isDupFlag)) {
                sendPubAck(channel, packetId, isDupFlag);
            } else {
                LOGGER.info("Failed to insert MQTT message into CrateDB: {}", t.getMessage());
            }
        }

        private static void sendPubAck(Channel channel, int packetId, boolean isDupFlag) {
            channel.writeAndFlush(MqttMessageBuilders.pubAck()
                    .qos(MqttQoS.AT_LEAST_ONCE)
                    .isDup(isDupFlag)
                    .packetId(packetId)
                    .build())
                    .addListener(cf -> LOGGER.info("PUBACK sent"));
        }
    }
}
