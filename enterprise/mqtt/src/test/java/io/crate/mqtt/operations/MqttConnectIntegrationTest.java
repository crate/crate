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

import io.crate.mqtt.netty.Client;
import io.crate.mqtt.protocol.CrateMqttMessageBuilders;
import io.netty.handler.codec.mqtt.*;
import org.junit.Test;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static org.hamcrest.Matchers.is;


public class MqttConnectIntegrationTest extends MqttIntegrationTest {

    private static MqttMessage connectMessage(String clientId, boolean isCleanSession) {
        return CrateMqttMessageBuilders.connect()
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .clientId(clientId)
                .cleanSession(isCleanSession)
                .build();
    }

    @Test
    public void testConnectWithoutClientId() throws Exception {
        MqttMessage connectMessage = connectMessage("", false);

        MQTT_CLIENT.sendMessage(connectMessage);
        MqttConnAckMessage response = (MqttConnAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_REFUSED_IDENTIFIER_REJECTED));
    }

    @Test
    public void testConnectWithoutClientIdButCleanSession() throws Exception {
        MqttMessage connectMessage = connectMessage("", true);

        MQTT_CLIENT.sendMessage(connectMessage);
        MqttConnAckMessage response = (MqttConnAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_ACCEPTED));
    }

    @Test
    public void testConnect() throws Exception {
        MqttMessage connectMessage = connectMessage("c1", true);

        MQTT_CLIENT.sendMessage(connectMessage);
        MqttConnAckMessage response = (MqttConnAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_ACCEPTED));
    }

    @Test
    public void testConcurrentConnections() throws Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        final AtomicReference<Throwable> lastThrowed = new AtomicReference<>();

        for(int i = 0; i < 50; i++) {
            String clientName = String.format("c%s", i);
            executorService.submit(() -> {
                try {
                    Client mqttClientThread = new Client("127.0.0.1", MQTT_PORT);
                    mqttClientThread.connect();

                    MqttMessage connectMessage = connectMessage(clientName, true);

                    mqttClientThread.sendMessage(connectMessage);

                    MqttConnAckMessage response = (MqttConnAckMessage) mqttClientThread.lastReceivedMessage();
                    assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_ACCEPTED));
                    mqttClientThread.close();
                } catch (Throwable t) {
                    lastThrowed.set(t);
                }
            });
        }

        executorService.shutdown();

        Throwable throwable = lastThrowed.get();
        if (throwable != null) {
            throw throwable;
        }
    }

}
