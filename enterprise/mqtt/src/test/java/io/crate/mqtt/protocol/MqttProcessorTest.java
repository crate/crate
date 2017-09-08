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

import io.crate.mqtt.netty.MqttMessages;
import io.crate.mqtt.operations.MqttIngestService;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class MqttProcessorTest {

    private final MqttProcessor processor = new MqttProcessor(mock(MqttIngestService.class));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static MqttMessage connectMessage(String clientId, boolean isCleanSession) {
        return MqttMessages.connectBuilder()
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .clientId(clientId)
                .cleanSession(isCleanSession)
                .build();
    }

    @Test
    public void testConnectWithWrongMqttVersion() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();

        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNECT, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                "connect", (byte) 1, false, false, false, (byte) 1, false, false, 60);
        MqttConnectPayload payload = new MqttConnectPayload("mqttClient", "someTopic", new byte[0], null, null);
        processor.handleConnect(ch,
                (MqttConnectMessage) io.netty.handler.codec.mqtt.MqttMessageFactory.newMessage(fixedHeader, variableHeader, payload));

        MqttConnAckMessage response = ch.readOutbound();
        assertThat(response.variableHeader().connectReturnCode(),
                is(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION));
        assertFalse(response.variableHeader().isSessionPresent());
    }

    @Test
    public void testConnectWithoutClientId() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();

        // clientId may be null if the session is clean
        MqttMessage msg = connectMessage(null, true);
        processor.handleConnect(ch, (MqttConnectMessage) msg);

        MqttConnAckMessage response = ch.readOutbound();
        assertThat(response.variableHeader().connectReturnCode(),
                is(MqttConnectReturnCode.CONNECTION_ACCEPTED));
        assertTrue(response.variableHeader().isSessionPresent());

        // clientID must not be null if the session is not clean
        msg = connectMessage(null, false);
        processor.handleConnect(ch, (MqttConnectMessage) msg);

        response = ch.readOutbound();
        assertThat(response.variableHeader().connectReturnCode(),
                is(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED));
        assertFalse(response.variableHeader().isSessionPresent());
    }

    @Test
    public void testConnectAck() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();

        MqttMessage msg = connectMessage("client", true);
        processor.handleConnect(ch, (MqttConnectMessage) msg);

        MqttConnAckMessage response = ch.readOutbound();
        assertThat(response.variableHeader().connectReturnCode(),
                is(MqttConnectReturnCode.CONNECTION_ACCEPTED));
        assertTrue(response.variableHeader().isSessionPresent());
    }
}
