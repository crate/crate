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

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.Session;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.ingestion.IngestRuleListener;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.table.Operation;
import io.crate.expression.InputFactory;
import io.crate.expression.RowFilter;
import io.crate.auth.user.User;
import io.crate.auth.user.UserLookup;
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
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.transport.netty4.Netty4Utils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * MQTT ingestion implementation. Handles mqtt messages by extracting the {@link MqttIngestService#MQTT_FIELDS_ORDER}
 * fields, matching the existing {@link IngestRule}s against the fields and ultimately executing the rule command
 * against the configured target table. The target table structure needs to contain the {@link MqttIngestService#MQTT_FIELDS_ORDER}
 * fields with the appropriate data types (see {@link MqttIngestService#FIELD_TYPES}.
 */
public class MqttIngestService implements IngestRuleListener {

    private static final Logger LOGGER = Loggers.getLogger(MqttIngestService.class);
    public static final String SOURCE_IDENT = "mqtt";
    private static final Map<QualifiedName, Integer> MQTT_FIELDS_ORDER = ImmutableMap.of(new QualifiedName("client_id"), 0,
        new QualifiedName("packet_id"), 1,
        new QualifiedName("topic"), 2,
        new QualifiedName("ts"), 3,
        new QualifiedName("payload"), 4);
    private static final List<DataType> FIELD_TYPES = Arrays.asList(DataTypes.STRING, DataTypes.INTEGER, DataTypes.STRING, DataTypes.OBJECT);
    private static final Predicate<Row> ALWAYS_TRUE = (r) -> true;

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
                return new InputColumn(MQTT_FIELDS_ORDER.get(qualifiedName));
            }
        };
        this.expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            new TransactionContext(),
            null,
            mqttSourceFieldsProvider,
            null);
        this.ingestionService = ingestionService;
        this.crateUser = userLookup.findUser("crate");
    }

    /**
     * Registers the service as an #{@link IngestRuleListener} with the #{@link IngestionService}.
     * This must only be called once.
     */
    public void initialize() {
        if (isInitialized) {
            throw new IllegalStateException("Service already initialized");
        }
        ingestionService.registerIngestRuleListener(SOURCE_IDENT, this);
        isInitialized = true;
    }

    @Nullable
    private static Map<String, Object> parsePayloadToMap(ByteBuf payload) {
        try {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                Netty4Utils.toBytesReference(payload)).map();
        } catch (IOException e) {
            LOGGER.warn("Failed to parse payload to JSON: {}", e.getMessage());
        }
        // null indicates parsing error
        return null;
    }

    /**
     * Parse the payload of the provided mqtt message and find which {@link IngestRule}s match the message properties.
     * For every matched rule, execute the insert into the rule's target table.
     * If we encounter exceptions whilst executing the rules we try to send the ackCallback that failure that's not a
     * "row already exists" (because, in case of messages with the isDup flag set to true, we will send the PUBACK reply
     * to the message).
     * We do not want to acknowledge the message until all message rules are successfully applied (if
     * the only detected failure(s) is "row already exists", namely {@link VersionConflictEngineException}, we trigger
     * the ackCallback, as for QoS 1 (at least once) we expect message redeliveries)
     */
    public void doInsert(String clientId, MqttPublishMessage msg, BiConsumer<Object, Throwable> ackCallback) {
        if (isInitialized == false) {
            throw new IllegalStateException("Service was not initialized");
        }

        Map<String, Object> payload = parsePayloadToMap(msg.content());
        if (payload == null) {
            return;
        }

        Set<Tuple<Predicate<Row>, IngestRule>> predicateAndIngestRules = predicateAndIngestRulesReference.get();
        int packetId = msg.variableHeader().packetId();
        Object[] args = new Object[]{clientId,
            packetId,
            msg.variableHeader().topicName(),
            payload};
        List<Object> argsAsList = Arrays.asList(args);

        boolean messageMatchedRule = false;
        boolean callbackNotified = false;
        Session session = sqlOperations.createSession(Schemas.DOC_SCHEMA_NAME, crateUser, Option.NONE, 1);
        List<CompletableFuture<?>> insertOperationsFuture = new ArrayList<>(predicateAndIngestRules.size());
        for (Tuple<Predicate<Row>, IngestRule> entry : predicateAndIngestRules) {
            if (entry.v1().test(new RowN(args))) {
                messageMatchedRule = true;
                IngestRule ingestRule = entry.v2();

                try {
                    session.parse(ingestRule.getName(), "insert into " + TableIdent.fromIndexName(ingestRule.getTargetTable()).fqn() +
                                                  " (\"client_id\", \"packet_id\", \"topic\", \"ts\", \"payload\") " +
                                                  "values (?, ?, ?, CURRENT_TIMESTAMP, ?)", FIELD_TYPES);
                    session.bind(Session.UNNAMED, ingestRule.getName(), argsAsList, null);
                    BaseResultReceiver resultReceiver = new BaseResultReceiver();
                    insertOperationsFuture.add(resultReceiver.completionFuture().exceptionally(t -> {
                        if (SQLExceptions.isDocumentAlreadyExistsException(t)) {
                            if (msg.fixedHeader().isDup()) {
                                // we are dealing with QoS1, so redeliveries and duplicate insert exceptions are
                                // normal in case of a duplicate message - indicated by the isDup flag
                                return null;
                            }
                        }

                        Exceptions.rethrowUnchecked(t);
                        return null;
                    }));
                    session.execute(Session.UNNAMED, 0, resultReceiver);
                    session.sync();
                } catch (SQLActionException e) {
                    ackCallback.accept(null, e);
                    callbackNotified = true;
                    break;
                }
            }
        }
        session.close();

        if (callbackNotified == false) {
            CompletableFuture<Void> allResultsComplete =
                CompletableFuture.allOf(insertOperationsFuture.toArray(new CompletableFuture[0]));

            allResultsComplete.whenComplete((r, t) -> {
                if (t != null) {
                    // the actual cause is wrapped in a CompletionException by CompletableFuture#allOf
                    ackCallback.accept(null, t.getCause());
                } else {
                    ackCallback.accept(r, null);
                }
            });
        }

        if (messageMatchedRule == false) {
            LOGGER.warn("Message with client_id {} and packet_id {} did not match any rule. The message will not be " +
                        "acknowledged", clientId, packetId);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Parsed MQTT message into arguments: {}", argsAsList);
        }
    }

    @Override
    public void applyRules(Set<IngestRule> rules) {
        Set<Tuple<Predicate<Row>, IngestRule>> newRules = new HashSet<>(rules.size());
        for (IngestRule rule : rules) {
            if (rule.getCondition().trim().isEmpty() == false) {
                Symbol conditionSymbol = expressionAnalyzer.convert(SqlParser.createExpression(rule.getCondition()),
                    expressionAnalysisContext);
                Predicate<Row> conditionPredicate = RowFilter.create(inputFactory, conditionSymbol);
                newRules.add(new Tuple<>(conditionPredicate, rule));
            } else {
                newRules.add(new Tuple<>(ALWAYS_TRUE, rule));
            }
        }
        predicateAndIngestRulesReference.set(newRules);
    }
}
