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
import io.crate.mqtt.netty.MqttMessages;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_ACCEPTED;
import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
import static org.hamcrest.Matchers.is;


public class MqttConnectIntegrationTest extends MqttIntegrationTest {

    private static MqttMessage connectMessage(String clientId, boolean isCleanSession) {
        return MqttMessages.connectBuilder()
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .clientId(clientId)
                .cleanSession(isCleanSession)
                .build();
    }

    @Test
    public void testConnectWithoutClientId() throws Exception {
        MqttMessage connectMessage = connectMessage("", false);

        mqttClient.sendMessage(connectMessage);
        MqttConnAckMessage response = (MqttConnAckMessage) mqttClient.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_REFUSED_IDENTIFIER_REJECTED));
    }

    @Test
    public void testConnectWithoutClientIdButCleanSession() throws Exception {
        MqttMessage connectMessage = connectMessage("", true);

        mqttClient.sendMessage(connectMessage);
        MqttConnAckMessage response = (MqttConnAckMessage) mqttClient.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_ACCEPTED));
    }

    @Test
    public void testConnect() throws Exception {
        MqttMessage connectMessage = connectMessage("c1", true);

        mqttClient.sendMessage(connectMessage);
        MqttConnAckMessage response = (MqttConnAckMessage) mqttClient.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_ACCEPTED));
    }

    @Test
    public void testConcurrentConnections() throws Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        final AtomicReference<Throwable> lastThrowed = new AtomicReference<>();

        for(int i = 0; i < 50; i++) {
            String clientName = String.format("c%s", i);
            executorService.submit(() -> {
                try (Client mqttClient = createMqttClient()) {
                    mqttClient.connect();
                    MqttMessage connectMessage = connectMessage(clientName, true);
                    mqttClient.sendMessage(connectMessage);

                    MqttConnAckMessage response = (MqttConnAckMessage) mqttClient.lastReceivedMessage();
                    assertThat(response.variableHeader().connectReturnCode(), is(CONNECTION_ACCEPTED));
                } catch (Throwable t) {
                    lastThrowed.set(t);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        Throwable throwable = lastThrowed.get();
        if (throwable != null) {
            throw throwable;
        }
    }
}
