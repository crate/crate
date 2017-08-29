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
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.ingestion.IngestionImplementation;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.table.Operation;
import io.crate.operation.InputFactory;
import io.crate.operation.RowFilter;
import io.crate.operation.user.User;
import io.crate.operation.user.UserLookup;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * MQTT ingestion implementation. Handles mqtt messages by extracting the {@link MqttIngestService#MQTT_FIELDS_ORDER}
 * fields, matching the existing {@link IngestRule}s against the fields and ultimately executing the rule command
 * against the configured target table. The target table structure needs to contain the {@link MqttIngestService#MQTT_FIELDS_ORDER}
 * fields with the appropriate data types (see {@link MqttIngestService#DATA_TYPES}.
 */
public class MqttIngestService implements IngestionImplementation {

    private static final Logger LOGGER = Loggers.getLogger(MqttIngestService.class);
    static final String SOURCE_IDENT = "mqtt";
    private static final Map<String, Integer> MQTT_FIELDS_ORDER = ImmutableMap.of("client_id", 0,
        "packet_id", 1,
        "topic", 2,
        "ts", 3,
        "payload", 4);
    private static final List<DataType> DATA_TYPES = Arrays.asList(DataTypes.STRING, DataTypes.INTEGER, DataTypes.STRING, DataTypes.OBJECT);
    private static final String STATEMENT_NAME = "ingestMqtt";

    private final ExpressionAnalyzer expressionAnalyzer;
    private final InputFactory inputFactory;
    private final SQLOperations sqlOperations;
    private final IngestionService ingestionService;
    private final User crateUser;
    private final AtomicReference<Set<Tuple<Predicate<Row>, IngestRule>>> predicateAndIngestRulesReference =
        new AtomicReference<>(new HashSet<>());
    private final ExpressionAnalysisContext expressionAnalysisContext;
    private boolean isInitialized;

    public MqttIngestService(Functions functions,
                             SQLOperations sqlOperations,
                             UserLookup userLookup,
                             IngestionService ingestionService) {
        this.sqlOperations = sqlOperations;
        this.inputFactory = new InputFactory(functions);
        this.expressionAnalysisContext = new ExpressionAnalysisContext();
        FieldProvider<Symbol> mqttSourceFieldsProvider = new FieldProvider<Symbol>() {

            @Override
            public Symbol resolveField(QualifiedName qualifiedName, Operation operation) {
                return resolveField(qualifiedName, null, operation);
            }

            @Override
            public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
                return new InputColumn(MQTT_FIELDS_ORDER.get(qualifiedName.toString()));
            }
        };
        this.expressionAnalyzer = new ExpressionAnalyzer(functions, null, null, mqttSourceFieldsProvider, null);
        this.ingestionService = ingestionService;
        this.crateUser = userLookup.findUser("crate");
    }

    public void initialize() {
        if (isInitialized) {
            throw new IllegalStateException("Service already initialized");
        }
        ingestionService.registerImplementation(SOURCE_IDENT, this);
        isInitialized = true;
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
        if (isInitialized == false) {
            throw new IllegalStateException("Service was not initialized");
        }

        Map<String, Object> payload = parsePayloadToMap(msg.content());
        if (payload == null) {
            return;
        }

        Set<Tuple<Predicate<Row>, IngestRule>> predicateAndIngestRules = predicateAndIngestRulesReference.get();
        Object[] args = new Object[]{clientId,
            msg.variableHeader().packetId(),
            msg.variableHeader().topicName(),
            payload};
        List<Object> argsAsList = Arrays.asList(args);

        for (Tuple<Predicate<Row>, IngestRule> entry : predicateAndIngestRules) {
            if (entry.v1().test(new RowN(args))) {
                IngestRule ingestRule = entry.v2();
                SQLOperations.Session session = createSessionFor(TableIdent.fromIndexName(ingestRule.getTargetTable()));
                session.bind(SQLOperations.Session.UNNAMED, STATEMENT_NAME, argsAsList, null);
                ResultReceiver resultReceiver = new BaseResultReceiver();
                resultReceiver.completionFuture().whenComplete(ackCallback);
                session.execute(SQLOperations.Session.UNNAMED, 0, resultReceiver);
                session.sync();
                session.close();
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Parsed MQTT message into arguments: {}", argsAsList);
        }
        ResultReceiver resultReceiver = new BaseResultReceiver();
        resultReceiver.completionFuture().whenComplete(ackCallback);
    }

    @Override
    public void applyRules(Set<IngestRule> rules) {
        Set<Tuple<Predicate<Row>, IngestRule>> newRules = new HashSet<>(rules.size());
        for (IngestRule rule : rules) {
            Symbol conditionSymbol = expressionAnalyzer.convert(SqlParser.createExpression(rule.getCondition()),
                expressionAnalysisContext);
            Predicate<Row> conditionPredicate = RowFilter.create(inputFactory, conditionSymbol);
            newRules.add(new Tuple<>(conditionPredicate, rule));
        }
        predicateAndIngestRulesReference.set(newRules);
    }

    private SQLOperations.Session createSessionFor(TableIdent tableIdent) {
        SQLOperations.Session session = sqlOperations.createSession(tableIdent.schema(), crateUser, Option.NONE, 1);
        session.parse(STATEMENT_NAME, "insert into " + tableIdent.fqn() +
                                      " (\"client_id\", \"packet_id\", \"topic\", \"ts\", \"payload\") " +
                                      "values (?, ?, ?, CURRENT_TIMESTAMP, ?)", DATA_TYPES);
        return session;
    }
}
