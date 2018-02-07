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

import io.crate.action.sql.SQLOperations;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.protocols.postgres.BindPostgresException;
import io.crate.protocols.ssl.SslContextProvider;
import io.crate.settings.SharedSettings;
import io.crate.test.integration.CrateUnitTest;
import io.crate.user.StubUserManager;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.BindTransportException;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.UnknownHostException;
import java.util.Collections;

import static io.crate.mqtt.netty.Netty4MqttServerTransport.MQTT_ENABLED_SETTING;
import static java.net.InetAddress.getByName;
import static org.hamcrest.Matchers.instanceOf;

public class MqttNettyNetworkConfigTest extends CrateUnitTest {

    private static Settings.Builder createBaseSettings() {
        return Settings.builder()
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), "true")
            .put(MQTT_ENABLED_SETTING.getKey(), "true");
    }

    @Test
    public void testBindAndPublishAddressDefault() {
        // First check if binding to a local works
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Netty4MqttServerTransport mqtt = new Netty4MqttServerTransport(createBaseSettings().build(), networkService,
            Mockito.mock(Functions.class), Mockito.mock(SQLOperations.class), new StubUserManager(),
            Mockito.mock(IngestionService.class), Mockito.mock(SslContextProvider.class));
        try {
            mqtt.doStart();
        } finally {
            mqtt.doStop();
        }
    }

    @Test
    public void testGeneralBindAndPublishAddressOverrideSetting() {
        // Check override for network.host
        Settings settingsWithCustomHost = createBaseSettings().put("network.host", "cantbindtothis").build();
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Netty4MqttServerTransport mqtt = new Netty4MqttServerTransport(settingsWithCustomHost, networkService,
            Mockito.mock(Functions.class), Mockito.mock(SQLOperations.class), new StubUserManager(),
            Mockito.mock(IngestionService.class), Mockito.mock(SslContextProvider.class));
        try {
            mqtt.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindPostgresException e) {
            // that's what we want
            assertThat(e.getCause(), instanceOf(UnknownHostException.class));
        } finally {
            mqtt.doStop();
        }
    }

    @Test
    public void testBindAddressOverrideSetting() {
        // Check override for network.bind_host
        Settings settingsWithCustomBind = createBaseSettings().put("network.bind_host", "cantbindtothis").build();
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Netty4MqttServerTransport mqtt = new Netty4MqttServerTransport(settingsWithCustomBind, networkService,
            Mockito.mock(Functions.class), Mockito.mock(SQLOperations.class), new StubUserManager(),
            Mockito.mock(IngestionService.class), Mockito.mock(SslContextProvider.class));
        try {
            mqtt.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindPostgresException e) {
            // that's what we want
            assertThat(e.getCause(), instanceOf(UnknownHostException.class));
        } finally {
            mqtt.doStop();
        }
    }

    @Test
    public void testPublishAddressOverride() {
        // Check override for network.publish_host
        Settings settingsWithCustomPublish = createBaseSettings().put("network.publish_host", "cantbindtothis").build();
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Netty4MqttServerTransport mqtt = new Netty4MqttServerTransport(settingsWithCustomPublish, networkService,
            Mockito.mock(Functions.class), Mockito.mock(SQLOperations.class), new StubUserManager(),
            Mockito.mock(IngestionService.class), Mockito.mock(SslContextProvider.class));
        try {
            mqtt.doStart();
            fail("Should have failed due to custom hostname");
        } catch (BindTransportException e) {
            // that's what we want
            assertThat(e.getCause(), instanceOf(UnknownHostException.class));
        } finally {
            mqtt.doStop();
        }
    }

    private TransportAddress address(String host, int port) throws UnknownHostException {
        return new TransportAddress(getByName(host), port);
    }
}
