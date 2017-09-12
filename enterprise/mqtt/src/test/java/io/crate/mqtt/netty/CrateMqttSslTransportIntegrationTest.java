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
 */

package io.crate.mqtt.netty;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.mqtt.operations.MqttIngestService;
import io.crate.protocols.ssl.SslConfigSettings;
import io.crate.settings.SharedSettings;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;

import static org.hamcrest.Matchers.is;


@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class CrateMqttSslTransportIntegrationTest extends SQLTransportIntegrationTest {

    private static File trustStoreFile;
    private static File keyStoreFile;
    private MqttClient client;

    @BeforeClass
    public static void beforeIntegrationTest() throws IOException {
        keyStoreFile = getFile(CrateMqttSslTransportIntegrationTest.class.getClassLoader(), "keystore.jks");
        trustStoreFile = getFile(CrateMqttSslTransportIntegrationTest.class.getClassLoader(), "truststore.jks");
        System.setProperty("javax.net.ssl.trustStore", trustStoreFile.getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStorePassword", "truststorePassword");
    }

    private static File getFile(ClassLoader classLoader, String fileName) throws IOException {
        final URL fileUrl = classLoader.getResource(fileName);
        if (fileUrl == null) {
            throw new FileNotFoundException("Resource was not found: " + fileName);
        }
        return new File(URLDecoder.decode(fileUrl.getFile(), "UTF-8"));
    }

    @AfterClass
    public static void afterIntegrationTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SharedSettings.ENTERPRISE_LICENSE_SETTING.getKey(), true)
            .put(Netty4MqttServerTransport.SSL_MQTT_ENABLED.getKey(), true)
            .put(Netty4MqttServerTransport.MQTT_ENABLED_SETTING.getKey(), true)
            .put(Netty4MqttServerTransport.MQTT_PORT_SETTING.getKey(), 8883)
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile)
            .put(SslConfigSettings.SSL_KEYSTORE_PASSWORD.getKey(), "keystorePassword")
            .put(SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.getKey(), "serverKeyPassword")
            .put(SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.getKey(), trustStoreFile)
            .put(SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.getKey(), "truststorePassword")
            .build();
    }

    @Before
    public void setupMqttTableAndIngestRule() {
        execute("CREATE TABLE IF NOT EXISTS mqtt.raw (\n" +
                "  client_id STRING,\n" +
                "  packet_id INTEGER,\n" +
                "  topic STRING,\n" +
                "  ts TIMESTAMP,\n" +
                "  payload OBJECT(DYNAMIC),\n" +
                "  PRIMARY KEY (\"client_id\", \"packet_id\")\n" +
                ") WITH (column_policy = 'strict', number_of_replicas = '0')");

        execute("CREATE INGEST RULE ingest_topic_t1 ON " + MqttIngestService.SOURCE_IDENT +
                " WHERE topic = ? INTO mqtt.raw", new Object[]{"t1"});
    }

    @Before
    public void setupMqttClient() throws MqttException {
        client = new MqttClient("ssl://localhost:8883", "sslTestClient", null);
        client.setTimeToWait(5000);
        MqttConnectOptions options = new MqttConnectOptions();
        options.setConnectionTimeout(60);
        options.setKeepAliveInterval(60);
        options.setSocketFactory(SSLSocketFactory.getDefault());
        client.connectWithResult(options).waitForCompletion();
    }

    @After
    public void dropIngestRule() {
        execute("DROP INGEST RULE IF EXISTS ingest_topic_t1");
    }

    @After
    public void disconnectAndCloseMqttClient() throws MqttException {
        client.disconnect();
        client.close();
    }

    @Test
    public void testPublishEncryptedMessage() throws Exception {
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // for QoS1 this is called when PUBACK is received (ie. the message was processed successfully)
                execute("select payload['id'] from mqtt.raw");
                assertThat(response.rowCount(), is(1L));
                assertThat(response.rows()[0][0], is(123L));
            }
        });

        client.publish("t1", "{\"id\": 123}".getBytes(), 1, false);
    }
}
