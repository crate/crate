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

package io.crate.iothub.operations;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.auth.user.User;
import io.crate.auth.user.UserLookup;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.Exceptions;
import io.crate.expression.InputFactory;
import io.crate.expression.RowFilter;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.ingestion.IngestRuleListener;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class EventIngestService implements IngestRuleListener {

    private static final Logger LOGGER = Loggers.getLogger(EventIngestService.class);
    public static final String SOURCE_IDENT = "iot_hub";
    private static final Map<QualifiedName, Integer> EVENT_HUB_FIELDS_ORDER = new ImmutableMap.Builder<QualifiedName, Integer>()
        .put(new QualifiedName("partition_context"), 0)
        .put(new QualifiedName("event_metadata"), 0)
        .put(new QualifiedName("ts"), 3)
        .put(new QualifiedName("payload"), 4)
        .build();
    private static final List<DataType> FIELD_TYPES = Arrays.asList(
        DataTypes.OBJECT,
        DataTypes.OBJECT,
        DataTypes.STRING,
        DataTypes.STRING);
    private static final Predicate<Row> ALWAYS_TRUE = (r) -> true;

    private final ExpressionAnalyzer expressionAnalyzer;
    private final InputFactory inputFactory;
    private final SQLOperations sqlOperations;
    private final IngestionService ingestionService;
    private final User crateUser;
    private final ExpressionAnalysisContext expressionAnalysisContext;
    private final AtomicReference<Set<Tuple<Predicate<Row>, IngestRule>>> predicateAndIngestRulesReference =
        new AtomicReference<>(new HashSet<>());
    private boolean isInitialized;

    public EventIngestService(Functions functions,
                              SQLOperations sqlOperations,
                              UserLookup userLookup,
                              IngestionService ingestionService) {
        this.sqlOperations = sqlOperations;
        this.inputFactory = new InputFactory(functions);
        this.expressionAnalysisContext = new ExpressionAnalysisContext();
        FieldProvider<Symbol> eventFieldsProvider = (qualifiedName, path, operation) -> new InputColumn(EVENT_HUB_FIELDS_ORDER.get(qualifiedName));
        this.expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            TransactionContext.systemTransactionContext(),
            null,
            eventFieldsProvider,
            null);
        this.ingestionService = ingestionService;
        this.crateUser = userLookup.findUser("crate");
    }

    public void initalize() {
        if (isInitialized) {
            throw new IllegalStateException("IoT Hub Ingestion Service already initialized.");
        }
        ingestionService.registerIngestRuleListener(SOURCE_IDENT, this);
        isInitialized = true;
    }

    @Nullable
    private static String payloadToString(byte[] payload) {
        try {
            return new String(payload, "UTF8");
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Unsupported Encoding!!! Oh no.");
            return null;
        }
    }

    private static Map<String, Object> partitionContextToMap(PartitionContext context) {
        Map<String, Object> contextMap = new HashMap<>();
        contextMap.put("event_hub_path", context.getEventHubPath());
        contextMap.put("consumer_group_name", context.getConsumerGroupName());
        contextMap.put("owner", context.getOwner());
        contextMap.put("partition_id", context.getPartitionId());
        return contextMap;
    }

    private static Map<String, Object> eventMetadataToMap(EventData.SystemProperties metadata) {
        Map<String, Object> metadataMap = new HashMap<>();
        metadataMap.put("sequence_number", metadata.getSequenceNumber());
        metadataMap.put("partition_key", metadata.getPartitionKey());
        metadataMap.put("publisher", metadata.getPublisher());
        metadataMap.put("offset", metadata.getOffset());
        metadataMap.put("enqueued_time", metadata.getEnqueuedTime().toString());
        return metadataMap;
    }

    public void doInsert(PartitionContext context, EventData data) {
        String payload = payloadToString(data.getBytes());
        if (payload == null) {
            return;
        }
        Object[] args = new Object[]{
            partitionContextToMap(context),
            eventMetadataToMap(data.getSystemProperties()),
            payload};
        List<Object> argsAsList = Arrays.asList(args);
        Session session = sqlOperations.createSession(Schemas.DOC_SCHEMA_NAME, crateUser, Option.NONE, 1);
        AtomicBoolean messageMatchedRule = new AtomicBoolean(false);
        predicateAndIngestRulesReference.get().stream().forEach(entry -> {
            if (entry.v1().test(new RowN(args))) {
                messageMatchedRule.set(true);
                IngestRule ingestRule = entry.v2();

                session.parse(ingestRule.getName(), "insert into " + RelationName.fromIndexName(ingestRule.getTargetTable()).fqn() +
                                               " (\"partition_context\", \"event_metadata\", \"ts\", \"payload\") " +
                                               "values (?, ?, CURRENT_TIMESTAMP, ?)", FIELD_TYPES);
                session.bind(Session.UNNAMED, ingestRule.getName(), argsAsList, null);
                BaseResultReceiver resultReceiver = new BaseResultReceiver();
                resultReceiver.completionFuture().exceptionally(t -> {
                    Exceptions.rethrowUnchecked(t);
                    return null;
                });
                session.execute(Session.UNNAMED, 0, resultReceiver);
                session.sync();
            }
        });
        session.close();

        if (!messageMatchedRule.get()) {
            LOGGER.warn("Event ({}, {}, {}) did not match any ingestion rule. The event will not be ingested.",
                        context.getPartitionId(), data.getSystemProperties().getOffset(), data.getSystemProperties().getSequenceNumber());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Event ({}, {}, {}): {}", context.getPartitionId(), data.getSystemProperties().getOffset(),
                         data.getSystemProperties().getSequenceNumber(), payload);
        }
    }

    @Override
    public void applyRules(Set<IngestRule> rules) {
        Set<Tuple<Predicate<Row>, IngestRule>> newRules = new HashSet<>(rules.size());
        rules.stream().forEach(rule -> {
            if (rule.getCondition().trim().isEmpty() == false) {
                Symbol conditionSymbol = expressionAnalyzer.convert(SqlParser.createExpression(rule.getCondition()),
                    expressionAnalysisContext);
                Predicate<Row> conditionPredicate = RowFilter.create(inputFactory, conditionSymbol);
                newRules.add(new Tuple<>(conditionPredicate, rule));
            } else {
                newRules.add(new Tuple<>(ALWAYS_TRUE, rule));
            }
        });
        predicateAndIngestRulesReference.set(newRules);
    }
}
