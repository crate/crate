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
 *
 * This code is derived from https://github.com/andsel/moquette/blob/master/broker/src/main/java/io/moquette/server/netty/metrics/MQTTMessageLogger.java
 */

package io.crate.mqtt.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

@ChannelHandler.Sharable
public class MqttMessageLogger extends ChannelDuplexHandler {

    private static final String DIRECTION_READ = "C->B";
    private static final String DIRECTION_WRITE = "C<-B";

    private final Logger logger;

    MqttMessageLogger(Settings settings) {
        logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        logMQTTMessage(ctx, message, DIRECTION_READ);
        ctx.fireChannelRead(message);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        logMQTTMessage(ctx, msg, DIRECTION_WRITE);
        ctx.write(msg, promise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String clientID = NettyUtils.clientID(ctx.channel());
        if (clientID != null && !clientID.isEmpty() && logger.isTraceEnabled()) {
            logger.trace("Channel closed <{}>", clientID);
        }
        ctx.fireChannelInactive();
    }

    private void logMQTTMessage(ChannelHandlerContext ctx, Object message, String direction) {
        if (logger.isTraceEnabled() && message instanceof MqttMessage) {
            MqttMessage msg = (MqttMessage) message;
            String clientID = NettyUtils.clientID(ctx.channel());
            MqttMessageType messageType = msg.fixedHeader().messageType();
            switch (messageType) {
                case CONNECT:
                case CONNACK:
                case PINGREQ:
                case PINGRESP:
                case DISCONNECT:
                    logger.trace("{} {} <{}>", direction, messageType, clientID);
                    break;
                case SUBSCRIBE:
                    MqttSubscribeMessage subscribe = (MqttSubscribeMessage) msg;
                    logger.trace("{} SUBSCRIBE <{}> to topics {}",
                        direction, clientID, subscribe.payload().topicSubscriptions());
                    break;
                case UNSUBSCRIBE:
                    MqttUnsubscribeMessage unsubscribe = (MqttUnsubscribeMessage) msg;
                    logger.trace("{} UNSUBSCRIBE <{}> to topics <{}>",
                        direction, clientID, unsubscribe.payload().topics());
                    break;
                case PUBLISH:
                    MqttPublishMessage publish = (MqttPublishMessage) msg;
                    logger.trace("{} PUBLISH <{}> to topics <{}>",
                        direction, clientID, publish.variableHeader().topicName());
                    break;
                case PUBREC:
                case PUBCOMP:
                case PUBREL:
                case PUBACK:
                case UNSUBACK:
                    logger.trace("{} {} <{}> packetID <{}>",
                        direction, messageType, clientID, messageId(msg));
                    break;
                case SUBACK:
                    MqttSubAckMessage suback = (MqttSubAckMessage) msg;
                    List<Integer> grantedQoSLevels = suback.payload().grantedQoSLevels();
                    logger.trace("{} SUBACK <{}> packetID <{}>, grantedQoses {}",
                        direction, clientID, messageId(msg), grantedQoSLevels);
                    break;
                default:
                    logger.trace("{} {} <{}> Unknown message type received.",
                        direction, messageType, clientID);
            }
        }
    }

    private static int messageId(MqttMessage msg) {
        return ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
    }
}
