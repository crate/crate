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

import io.netty.handler.codec.mqtt.*;

public final class MqttMessageBuilders {

    public static final class MqttPingResponseBuilder {

        private MqttQoS qos;

        MqttPingResponseBuilder() {
        }

        public MqttPingResponseBuilder qos(MqttQoS qos) {
            this.qos = qos;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, qos, false, 0);
            return new MqttMessage(mqttFixedHeader);
        }
    }

    public static final class MqttPingRequestBuilder {

        private MqttQoS qos;

        MqttPingRequestBuilder() {
        }

        public MqttPingRequestBuilder qos(MqttQoS qos) {
            this.qos = qos;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, qos, false, 0);
            return new MqttMessage(mqttFixedHeader);
        }
    }

    public static final class MqttDisconnectBuilder {

        private MqttQoS qos;

        MqttDisconnectBuilder() {
        }

        public MqttDisconnectBuilder qos(MqttQoS qos) {
            this.qos = qos;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, qos, false, 0);
            return new MqttMessage(mqttFixedHeader);
        }
    }

    public static final class MqttPubAckBuilder {

        private MqttQoS qos;
        private boolean isDup;
        private int packetId;

        MqttPubAckBuilder() {
        }

        public MqttPubAckBuilder qos(MqttQoS qos) {
            this.qos = qos;
            return this;
        }

        public MqttPubAckBuilder isDup(boolean isDup) {
            this.isDup = isDup;
            return this;
        }

        public MqttPubAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, isDup, qos, false, 0);
            MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);
            return new MqttMessage(mqttFixedHeader, variableHeader);
        }
    }

    public static io.netty.handler.codec.mqtt.MqttMessageBuilders.ConnectBuilder connect() {
        return io.netty.handler.codec.mqtt.MqttMessageBuilders.connect();
    }

    public static io.netty.handler.codec.mqtt.MqttMessageBuilders.ConnAckBuilder connAck() {
        return io.netty.handler.codec.mqtt.MqttMessageBuilders.connAck();
    }

    public static io.netty.handler.codec.mqtt.MqttMessageBuilders.PublishBuilder publish() {
        return io.netty.handler.codec.mqtt.MqttMessageBuilders.publish();
    }

    public static io.netty.handler.codec.mqtt.MqttMessageBuilders.SubscribeBuilder subscribe() {
        return io.netty.handler.codec.mqtt.MqttMessageBuilders.subscribe();
    }

    public static io.netty.handler.codec.mqtt.MqttMessageBuilders.UnsubscribeBuilder unsubscribe() {
        return io.netty.handler.codec.mqtt.MqttMessageBuilders.unsubscribe();
    }

    public static MqttPingRequestBuilder pingReq() {
        return new MqttPingRequestBuilder();
    }

    public static MqttPingResponseBuilder pingResp() {
        return new MqttPingResponseBuilder();
    }

    public static MqttDisconnectBuilder disconnect() {
        return new MqttDisconnectBuilder();
    }

    public static MqttPubAckBuilder pubAck() {
        return new MqttPubAckBuilder();
    }

}
