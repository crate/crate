package io.crate.mqtt.operations;

import io.crate.mqtt.netty.MqttMessages;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MqttPublishIntegrationTest extends MqttIntegrationTest {

    private static final AtomicInteger messageId = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        super.setUp();

        execute("CREATE TABLE IF NOT EXISTS mqtt.raw (\n" +
                "  client_id STRING,\n" +
                "  packet_id INTEGER,\n" +
                "  topic STRING,\n" +
                "  ts TIMESTAMP,\n" +
                "  payload OBJECT(IGNORED),\n" +
                "  PRIMARY KEY (\"client_id\", \"packet_id\")\n" +
                ") WITH (column_policy = 'strict', number_of_replicas = '0')");

        execute("CREATE INGEST RULE ingest_all ON " + MqttIngestService.SOURCE_IDENT +
                " WHERE topic = ? INTO mqtt.raw", new Object[]{"t1"});

        // MQTT CONNECT
        MQTT_CLIENT.sendMessage(MqttMessages.connectBuilder()
                .clientId(MQTT_CLIENT.clientId())
                .build());
        MqttConnAckMessage response = (MqttConnAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().connectReturnCode(),
                is(MqttConnectReturnCode.CONNECTION_ACCEPTED));
    }

    @After
    public void dropIngestRule() {
        execute("DROP INGEST RULE IF EXISTS ingest_all");
        execute("DROP INGEST RULE IF EXISTS rule_on_table_with_bad_schema");
    }

    private static MqttPublishMessage publishMessage(String payload) {
        return MqttMessages.publishBuilder()
            .topicName("t1")
            .qos(MqttQoS.AT_LEAST_ONCE)
            .payload(Unpooled.copiedBuffer(payload.getBytes()))
            .messageId(messageId.incrementAndGet())
            .build();
    }

    @Test
    public void testPublishUnsupportedQoS() throws Exception {
        MqttPublishMessage mqttPublishMessage = MqttMessages.publishBuilder()
                .qos(MqttQoS.AT_LEAST_ONCE)
                .topicName("t1")
                .messageId(messageId.incrementAndGet())
                .retained(false)
                .payload(Unpooled.copiedBuffer("Hello World".getBytes()))
                .build();
        MQTT_CLIENT.sendMessage(mqttPublishMessage);

        expectedException.expectMessage("MQTT client did not receive message in time");
        MQTT_CLIENT.lastReceivedMessage();
    }

    @Test
    public void testPublishQoS1WithNonJsonPayload() throws Exception {
        MqttMessage message = publishMessage("hello world");
        MQTT_CLIENT.sendMessage(message);

        expectedException.expectMessage("MQTT client did not receive message in time");
        MQTT_CLIENT.lastReceivedMessage();
    }

    @Test
    public void testPublishQos1WithoutDUPFlag() throws Exception {
        int mId = messageId.incrementAndGet();
        // isDup = false
        io.netty.handler.codec.mqtt.MqttMessageBuilders.PublishBuilder message = MqttMessages.publishBuilder()
                .topicName("t1")
                .qos(MqttQoS.AT_LEAST_ONCE)
                .payload(Unpooled.copiedBuffer("{\"ts\":1498797237000}".getBytes()))
                .messageId(mId);

        MQTT_CLIENT.sendMessage(message.build());
        MqttPubAckMessage response = (MqttPubAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().messageId(), is(mId));

        // resend same message with isDup = false
        MQTT_CLIENT.sendMessage(message.build());
        expectedException.expectMessage("MQTT client did not receive message in time");
        MQTT_CLIENT.lastReceivedMessage();
    }

    @Test
    public void testPublishQos1WithDUPFlag() throws Exception {
        // isDup = false
        MqttPublishMessage message = publishMessage("{\"ts\":1498797237000}");
        MQTT_CLIENT.sendMessage(message);
        MqttPubAckMessage response = (MqttPubAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().messageId(), is(message.variableHeader().packetId()));

        // isDup = true
        message = publishMessage("{\"ts\":1498797237000}");
        MQTT_CLIENT.sendMessage(message);
        response = (MqttPubAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().messageId(), is(message.variableHeader().packetId()));
    }

    @Test
    public void testPublishQoS1WithJsonPayload() throws Exception {
        MqttPublishMessage message = publishMessage("{}");
        MQTT_CLIENT.sendMessage(message);
        MqttPubAckMessage response = (MqttPubAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(response.variableHeader().messageId(), is(message.variableHeader().packetId()));
    }

    @Test
    public void testMessageInserted() throws Exception {
        MqttPublishMessage message = publishMessage("{\"ts\":1498797237000}");
        MQTT_CLIENT.sendMessage(message);
        MqttPubAckMessage mqttResponse = (MqttPubAckMessage) MQTT_CLIENT.lastReceivedMessage();
        assertThat(mqttResponse.variableHeader().messageId(), is(message.variableHeader().packetId()));

        execute("SELECT client_id, packet_id, topic, payload, ts " +
                "FROM mqtt.raw " +
                "WHERE client_id = ? AND packet_id = ?",
            new Object[]{MQTT_CLIENT.clientId(), message.variableHeader().packetId()});
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(MQTT_CLIENT.clientId()));
        assertThat(response.rows()[0][1], is(message.variableHeader().packetId()));
        assertThat(response.rows()[0][2], is(message.variableHeader().topicName()));
        assertThat(response.rows()[0][3], notNullValue());
        assertThat(response.rows()[0][4], notNullValue());
    }

    @Test
    public void testMessageIsNotAckedWhenARuleFailsToExecute() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS mqtt_with_bad_schema (\n" +
                "  topic INTEGER,\n" +
                "  ts TIMESTAMP,\n" +
                "  payload OBJECT(IGNORED)\n" +
                ") WITH (column_policy = 'strict', number_of_replicas = '0')");

        execute("CREATE INGEST RULE rule_on_table_with_bad_schema ON " + MqttIngestService.SOURCE_IDENT +
                " WHERE topic = ? INTO mqtt_with_bad_schema", new Object[]{"t1"});

        MqttPublishMessage message = publishMessage("{\"ts\":1498797237000}");
        MQTT_CLIENT.sendMessage(message);
        expectedException.expectMessage("MQTT client did not receive message in time (2000ms).");
        // we shouldn't have received an ack after sending the message as the execution of the rule rule_on_table_with_bad_schema
        // fails
        MQTT_CLIENT.lastReceivedMessage(2000);
    }
}
