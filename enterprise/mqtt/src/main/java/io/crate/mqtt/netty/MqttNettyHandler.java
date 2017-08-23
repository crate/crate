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

import io.crate.action.sql.SQLActionException;
import io.crate.mqtt.protocol.MqttProcessor;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Locale;


@Sharable
public class MqttNettyHandler extends ChannelInboundHandlerAdapter {

    private final Logger LOGGER = Loggers.getLogger(MqttNettyHandler.class);
    private final MqttProcessor processor;

    public MqttNettyHandler(MqttProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        MqttMessage msg = (MqttMessage) message;
        LOGGER.info("Received a message of type {}", msg.fixedHeader().messageType().toString());
        try {
            switch (msg.fixedHeader().messageType()) {
                case CONNECT:
                    processor.handleConnect(ctx.channel(), (MqttConnectMessage) msg);
                    break;
                case PUBLISH:
                    processor.handlePublish(ctx.channel(), (MqttPublishMessage) msg);
                    break;
                case DISCONNECT:
                    processor.handleDisconnect(ctx.channel());
                    break;
                case PINGREQ:
                    processor.handlePingReq(ctx.channel());
                    break;
                default:
                    String error = String.format(Locale.ENGLISH,
                            "Message type %s is not supported.", msg.fixedHeader().messageType().toString());
                    throw new UnsupportedOperationException(error);
            }
        } catch (Exception ex) {
            if (ex instanceof SQLActionException) {
                SQLActionException sqlActionException = (SQLActionException) ex;
                // 4041 is an unknown table exception, 4045 is an unknown schema exception
                if (sqlActionException.errorCode() == 4041 | sqlActionException.errorCode() == 4045) {
                    LOGGER.error("Unable to process MQTT message, as the table mqtt.raw does not exist");
                } else {
                    LOGGER.error("Unable to process MQTT message: ", sqlActionException.getDetailedMessage());
                }
            } else if (ex.getMessage() == null) {
                // print stacktrace if exception has no message
                LOGGER.error("Error processing MQTT message", ex);
            } else {
                // less verbose logging
                LOGGER.error("Error processing MQTT message: {}", ex.getMessage());
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("Uncaught exception:", cause);
    }

}
