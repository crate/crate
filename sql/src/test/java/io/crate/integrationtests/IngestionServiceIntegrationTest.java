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

package io.crate.integrationtests;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.ingestion.IngestRuleListener;
import io.crate.ingestion.IngestionService;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.table.Operation;
import io.crate.expression.InputFactory;
import io.crate.expression.RowFilter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.collect.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class IngestionServiceIntegrationTest extends SQLTransportIntegrationTest {

    private static final String INGESTION_SOURCE_ID = "testing";
    private static final String UNDER_TEST_INGEST_RULE_NAME = "ingest_data_rule";
    private static final String DATA_TOPIC = "test";
    private static final String TARGET_TABLE = "ingest_data_raw";
    private IngestionService ingestionService;

    private class TestIngestRuleListener implements IngestRuleListener {

        private final IngestionService ingestionService;
        private final InputFactory inputFactory;
        private final ExpressionAnalyzer expressionAnalyzer;
        private AtomicReference<Set<Tuple<Predicate<Row>, IngestRule>>> predicateAndIngestRulesReference;
        private final ExpressionAnalysisContext expressionAnalysisContext;
        private FieldProvider<Symbol> inputColumnProvider = new FieldProvider<Symbol>() {
            @Override
            public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
                return new InputColumn(0);
            }
        };

        TestIngestRuleListener(Functions functions,
                               IngestionService ingestionService) {
            this.predicateAndIngestRulesReference = new AtomicReference<>(new HashSet<>());
            this.inputFactory = new InputFactory(functions);
            this.ingestionService = ingestionService;
            this.expressionAnalysisContext = new ExpressionAnalysisContext();
            TransactionContext transactionContext = new TransactionContext(SessionContext.create());
            this.expressionAnalyzer = new ExpressionAnalyzer(
                functions, transactionContext, null, inputColumnProvider, null);
        }

        void registerListener() {
            ingestionService.registerIngestRuleListener(INGESTION_SOURCE_ID, this);
        }

        void ingestData(String topic, int data) {
            for (Tuple<Predicate<Row>, IngestRule> predicateAndRuleTuple : predicateAndIngestRulesReference.get()) {
                if (predicateAndRuleTuple.v1().test(new Row1(topic))) {
                    IngestRule ingestRule = predicateAndRuleTuple.v2();
                    execute("insert into " + ingestRule.getTargetTable() + " values (?);", new Object[]{data});
                    execute("refresh table " + ingestRule.getTargetTable());
                }
            }
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
    }

    private TestIngestRuleListener ruleListener;
    private Thread dataIngestionThread;
    private Iterator<Integer> dataSource = Stream.iterate(1, i -> i + 1).iterator();

    private volatile boolean produceData;
    private final AtomicInteger lastProducedData = new AtomicInteger(-1);
    private final Semaphore dataFlowSemaphore = new Semaphore(1);
    private volatile Set<String> existingRulesOnIngestValue;

    private void pauseDataIngestion() throws InterruptedException {
        dataFlowSemaphore.acquire();
    }

    private void resumeDataIngestion() {
        dataFlowSemaphore.release();
    }

    @Before
    public void setupIngestRuleAndListener() {
        execute(String.format(Locale.ENGLISH, "create table %s (data int)", TARGET_TABLE));
        execute(String.format(Locale.ENGLISH,
            "create ingest rule %s on %s where topic = ? into %s",
            UNDER_TEST_INGEST_RULE_NAME, INGESTION_SOURCE_ID, TARGET_TABLE),
            new Object[]{DATA_TOPIC}
        );
        ingestionService = internalCluster().getInstance(IngestionService.class);
        ruleListener = new TestIngestRuleListener(internalCluster().getInstance(Functions.class),
            ingestionService);
        ruleListener.registerListener();
        produceData = true;
        existingRulesOnIngestValue = new CopyOnWriteArraySet<>();
        dataIngestionThread = new Thread(() -> {
            while (produceData) {
                try {
                    // giving others a chance to acquire
                    Thread.sleep(50);
                    dataFlowSemaphore.acquire();
                    Integer nextValue = dataSource.next();

                    // Save the rules that the listener received before ingesting this value (and most likely will use
                    // to ingest it - unless tests do concurrent `drop ingest rule` but that's for the tests to handle)
                    // Tests will want to wait until we ingest values in the presence of particular rules, so we'll push
                    // these rules into a collection AFTER we do the ingest so that tests can assertBusy on tha collection
                    Set<Tuple<Predicate<Row>, IngestRule>> listenerReceivedRules =
                        ruleListener.predicateAndIngestRulesReference.get();

                    ruleListener.ingestData(DATA_TOPIC, nextValue);
                    lastProducedData.set(nextValue);

                    listenerReceivedRules.stream().map(tuple -> tuple.v2().getName())
                        .collect(toCollection(() -> existingRulesOnIngestValue));
                } catch (InterruptedException e) {
                    // will retry if interrupted
                } finally {
                    dataFlowSemaphore.release();
                }
            }
        }, "DataIngestionThread");
        dataIngestionThread.start();
    }

    @After
    public void dropIngestRuleAndStopProducingData() throws InterruptedException {
        execute("drop ingest rule if exists " + UNDER_TEST_INGEST_RULE_NAME);
        execute("drop ingest rule if exists another_topic_rule");
        produceData = false;
        resumeDataIngestion();
        dataIngestionThread.join(5000);
        ingestionService.removeListenerFor(INGESTION_SOURCE_ID);
    }

    @Test
    public void testIngestData() throws Exception {
        assertBusy(() -> assertThat(existingRulesOnIngestValue.contains(UNDER_TEST_INGEST_RULE_NAME), is(true)),
            5, TimeUnit.SECONDS);
        pauseDataIngestion();

        execute("select * from ingest_data_raw order by data desc");
        assertThat(response.rowCount(), greaterThan(0L));
        assertThat(response.rows()[0][0], is(lastProducedData.get()));
    }

    @Test
    public void testCreateRuleOnDifferentTopicThanIncomingData() throws Exception {
        execute("create table other_topic_raw_table (data int)");
        execute(String.format(Locale.ENGLISH,
            "create ingest rule another_topic_rule on %s where topic = '%s' into %s",
            INGESTION_SOURCE_ID, "other_topic", "other_topic_raw_table")
        );

        assertBusy(() -> assertThat(existingRulesOnIngestValue.contains("another_topic_rule"), is(true)),
            5, TimeUnit.SECONDS);
        execute("select * from other_topic_raw_table order by data desc");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testCreateMultipleRulesForTheSameTargetTable() throws Exception {
        execute(String.format(Locale.ENGLISH,
            "create ingest rule another_topic_rule on %s where topic = '%s' into %s",
            INGESTION_SOURCE_ID, DATA_TOPIC, TARGET_TABLE)
        );

        assertBusy(() -> assertThat(existingRulesOnIngestValue.contains("another_topic_rule"), is(true)),
            5, TimeUnit.SECONDS);
        pauseDataIngestion();

        // last produced data should've been ingested twice in the target table
        execute("select * from ingest_data_raw order by data desc limit 2");
        assertThat(response.rowCount(), greaterThan(1L));
        assertThat(response.rows()[0][0], is(lastProducedData.get()));
        assertThat(response.rows()[1][0], is(lastProducedData.get()));
    }

    @Test
    public void testDropIngestRuleWillCauseIngestionToStop() throws Exception {
        assertBusy(() -> assertThat(existingRulesOnIngestValue.contains(UNDER_TEST_INGEST_RULE_NAME), is(true)),
            5, TimeUnit.SECONDS);

        // pausing data ingestion so we can get the last ingested value before we drop the ingest rule.
        // we will assert that after the rule is dropped and ingestion resumed, no more data is inserted
        // into the target table, and this "last ingested value" is the last value inserted in the target table.
        pauseDataIngestion();
        int expectedLastIngestedValue = lastProducedData.get();
        execute("drop ingest rule " + UNDER_TEST_INGEST_RULE_NAME);

        // resuming data ingestion, but data that's flowing in after the rule was dropped should not be inserted into
        // the target table anymore
        resumeDataIngestion();

        execute("select * from ingest_data_raw order by data desc");
        assertThat(response.rowCount(), greaterThan(0L));
        assertThat(response.rows()[0][0], is(expectedLastIngestedValue));
    }
}
