/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
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
