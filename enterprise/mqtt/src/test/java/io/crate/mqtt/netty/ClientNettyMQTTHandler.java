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

import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttMessage;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author andrea
 */
class ClientNettyMQTTHandler extends ChannelInboundHandlerAdapter {

    private SettableFuture<MqttMessage> callback;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        if (callback != null) {
            callback.set((MqttMessage) message);
        }
        ctx.fireChannelRead(message);
    }

    public void initCallback() {
        if (callback != null) {
            callback.cancel(true);
        }
        callback = SettableFuture.create();
    }

    public MqttMessage lastMessage(long timeout) throws Exception {
        return callback.get(timeout, TimeUnit.MILLISECONDS);
    }
}
