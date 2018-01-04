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
 * Derived from https://github.com/andsel/moquette/blob/master/broker/src/main/java/io/moquette/server/netty/NettyUtils.java
 */

package io.crate.mqtt.netty;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * Static helpers for reading and writing netty attributes.
 */
public class NettyUtils {

    private static final AttributeKey<Object> ATTR_KEY_KEEPALIVE = AttributeKey.valueOf("keepAlive");
    private static final AttributeKey<Object> ATTR_KEY_CLIENTID = AttributeKey.valueOf("ClientID");

    public static void keepAlive(Channel channel, int keepAlive) {
        channel.attr(ATTR_KEY_KEEPALIVE).set(keepAlive);
    }

    public static void clientID(Channel channel, String clientID) {
        channel.attr(ATTR_KEY_CLIENTID).set(clientID);
    }

    public static String clientID(Channel channel) {
        return (String) channel.attr(ATTR_KEY_CLIENTID).get();
    }
}
