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

package io.crate.mqtt.operations;

import io.crate.mqtt.netty.MqttMessages;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class MqttPingIntegrationTest extends MqttIntegrationTest {

    @Test
    public void testPing() throws Exception {
        MQTT_CLIENT.sendMessage(MqttMessages.newPingRequest(MqttQoS.AT_LEAST_ONCE));
        MqttMessage response = MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.fixedHeader().messageType(), is(MqttMessageType.PINGRESP));
    }
}
