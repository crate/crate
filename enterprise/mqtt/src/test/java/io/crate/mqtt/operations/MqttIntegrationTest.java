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
import io.crate.mqtt.netty.Netty4MqttServerTransport;
import io.crate.operation.mqtt.MqttSettings;
import io.crate.settings.SharedSettings;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.After;
import org.junit.Before;

public abstract class MqttIntegrationTest extends SQLTransportIntegrationTest {

    private static final Logger LOGGER = Loggers.getLogger(MqttIntegrationTest.class);
    private static final String PORT_RANGE = "11883-11983";
    Client MQTT_CLIENT;
    private int port;

    Client createMqttClient() {
        LOGGER.info("Creating local MQTT client on port {}", port);
        return new Client("127.0.0.1", port);
    }

    @Before
    public void connectMqttClient() throws Exception {
        Netty4MqttServerTransport instance = internalCluster().getDataNodeInstance(Netty4MqttServerTransport.class);
        BoundTransportAddress boundAddress = instance.getBoundAddress();
        TransportAddress address = boundAddress.publishAddress();
        port = address.getPort();
        MQTT_CLIENT = createMqttClient();
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
            .put(MqttSettings.MQTT_ENABLED_SETTING.getKey(), true)
            .put(MqttSettings.MQTT_PORT_SETTING.getKey(), PORT_RANGE)
            .build();
    }
}
