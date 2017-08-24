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

import com.google.common.base.Charsets;
import io.crate.mqtt.netty.NettyUtils;
import io.crate.mqtt.operations.MqttIngestService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class MqttQualityOfServiceTest {

    private final MqttIngestService handler = mock(MqttIngestService.class);
    private final MqttProcessor processor = new MqttProcessor(handler);
    private final Channel ch = new EmbeddedChannel();

    @Before
    public void setUp() throws Exception {
        NettyUtils.clientID(ch, "c1");
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MqttPublishMessage messageWithQoS(MqttQoS level) {
        ByteBuf payload = Unpooled.copiedBuffer("{}".getBytes(Charsets.UTF_8));
        return MqttMessageBuilders.publish()
                .qos(level)
                .retained(false)
                .topicName("t1")
                .messageId(1)
                .payload(payload)
                .build();
    }

    @Test
    public void testProcessQoS0() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("QoS 0 (AT_MOST_ONCE) is not supported.");
        MqttPublishMessage message = messageWithQoS(MqttQoS.AT_MOST_ONCE);
        processor.handlePublish(ch, message);
    }

    @Test
    public void testProcessQoS1() throws Exception {
        MqttPublishMessage message = messageWithQoS(MqttQoS.AT_LEAST_ONCE);
        processor.handlePublish(ch, message);
        verify(handler, times(1)).doInsert(eq("c1"), eq(message), any(BiConsumer.class));
    }

    @Test
    public void testProcessQoS2() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("QoS 2 (EXACTLY_ONCE) is not supported.");
        MqttPublishMessage message = messageWithQoS(MqttQoS.EXACTLY_ONCE);
        processor.handlePublish(ch, message);
    }
}
