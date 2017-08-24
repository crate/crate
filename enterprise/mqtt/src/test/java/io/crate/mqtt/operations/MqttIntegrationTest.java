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

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.mqtt.netty.Client;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import static io.crate.mqtt.netty.Netty4MqttServerTransport.MQTT_ENABLED_SETTING;
import static io.crate.mqtt.netty.Netty4MqttServerTransport.MQTT_PORT_SETTING;

public abstract class MqttIntegrationTest extends SQLTransportIntegrationTest {

    static final int MQTT_PORT = 11883;
    static Client MQTT_CLIENT;

    @BeforeClass
    public static void setUpMqttClient() {
        MQTT_CLIENT = new Client("127.0.0.1", MQTT_PORT);
    }

    @Before
    public void connectMqttClient() throws Exception {
        MQTT_CLIENT.connect();
    }

    @After
    public void disconnectMqttClient() throws Exception {
        MQTT_CLIENT.disconnect();
        MQTT_CLIENT.close();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true)
            .put(MQTT_ENABLED_SETTING.getKey(), true)
            .put(MQTT_PORT_SETTING.getKey(), MQTT_PORT + nodeOrdinal)
            .build();
    }
}
