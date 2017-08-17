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

import com.google.common.base.Charsets;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.SQLOperations.Session;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;


public class CrateIngestService {

    private static final String DEFAULT_SCHEMA = "mqtt";
    private static final String STMT_NAME = "I";
    private static final Logger LOGGER = Loggers.getLogger(CrateIngestService.class);
    private final Session session;

    public CrateIngestService(SQLOperations sqlOperations) {
        List<DataType> argTypes = Arrays.asList(DataTypes.STRING, DataTypes.INTEGER, DataTypes.STRING, DataTypes.OBJECT);
        String stmt = "INSERT INTO raw (" +
                "\"client_id\", \"packet_id\", \"topic\", \"ts\", \"payload\"" +
                ") VALUES (" +
                "?, ?, ?, CURRENT_TIMESTAMP, ?" +
                ")";
        session = sqlOperations.createSession(DEFAULT_SCHEMA, null, Option.NONE, 1);
        session.parse(STMT_NAME, stmt, argTypes);
    }


   private static String mqttPayloadToString(ByteBuf content) {
        byte[] rawBytes;
        if (content.hasArray()) {
            rawBytes = content.array();
        } else {
            int size = content.readableBytes();
            rawBytes = new byte[size];
            content.getBytes(content.readerIndex(), rawBytes);
        }
        return new String(rawBytes, Charsets.UTF_8);
    }

    @Nullable
    private static Map<String, Object> parsePayloadToMap(ByteBuf payload) {
        String json = mqttPayloadToString(payload);
        try {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, json).mapOrdered();
        } catch (IOException e) {
            LOGGER.warn("Failed to parse payload [{}] to JSON: {}", json, e.getMessage());
        }
        // null indicates parsing error
        return null;
    }

    public void doInsert(String clientId, MqttPublishMessage msg, BiConsumer<Object, Throwable> ackCallback) {
        Map<String, Object> payload = parsePayloadToMap(msg.content());
        if (payload == null) {
            return;
        }
        List<Object> args = Arrays.asList(clientId,
                msg.variableHeader().packetId(),
                msg.variableHeader().topicName(),
                payload
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Parsed MQTT message into arguments: {}", args);
        }
        session.bind(Session.UNNAMED, STMT_NAME, args, null);
        ResultReceiver resultReceiver = new BaseResultReceiver();
        resultReceiver.completionFuture().whenComplete(ackCallback);
        session.execute(Session.UNNAMED, 0, resultReceiver);
        session.sync();
    }

    public void close() {
        session.close();
    }
}
