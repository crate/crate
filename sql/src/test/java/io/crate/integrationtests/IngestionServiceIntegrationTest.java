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

import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.metadata.Functions;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.rule.ingest.IngestionImplementation;
import io.crate.metadata.rule.ingest.IngestionService;
import io.crate.metadata.table.Operation;
import io.crate.operation.InputFactory;
import io.crate.operation.RowFilter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.collect.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.Matchers.is;

public class IngestionServiceIntegrationTest extends SQLTransportIntegrationTest {

    private static final String INGESTION_SOURCE_ID = "testing";
    private static final String UNDER_TEST_INGEST_RULE_NAME = "ingest_data_rule";
    private static final String DATA_TOPIC = "test";
    private static final String TARGET_TABLE = "ingest_data_raw";

    private class TestIngestionImplementation implements IngestionImplementation {

        private final IngestionService ingestionService;
        private final InputFactory inputFactory;
        private final ExpressionAnalyzer expressionAnalyzer;
        private AtomicReference<Set<Tuple<Predicate<Row>, IngestRule>>> predicateAndIngestRulesReference;
        private final ExpressionAnalysisContext expressionAnalysisContext;
        private FieldProvider<Symbol> inputColumnProvider = new FieldProvider<Symbol>() {

            @Override
            public Symbol resolveField(QualifiedName qualifiedName, Operation operation) {
                return resolveField(qualifiedName, null, operation);
            }

            @Override
            public Symbol resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
                return new InputColumn(0);
            }
        };

        TestIngestionImplementation(Functions functions,
                                    IngestionService ingestionService) {
            this.predicateAndIngestRulesReference = new AtomicReference<>(new HashSet<>());
            this.inputFactory = new InputFactory(functions);
            this.ingestionService = ingestionService;
            this.expressionAnalysisContext = new ExpressionAnalysisContext();
            Row args = new RowN($(DATA_TOPIC));
            ParameterContext ctx = new ParameterContext(args, Collections.emptyList());
            this.expressionAnalyzer = new ExpressionAnalyzer(functions, null, ctx, inputColumnProvider, null);
        }

        void registerIngestionImplementation() {
            ingestionService.registerImplementation(INGESTION_SOURCE_ID, this);
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

    private TestIngestionImplementation implementation;
    private Thread dataIngestionThread;
    private Iterator<Integer> dataSource = Stream.iterate(1, i -> i + 1).iterator();

    private volatile boolean produceData;
    private CountDownLatch dataFlowingLatch;
    private final AtomicInteger lastProducedData = new AtomicInteger(-1);
    private final Semaphore dataFlowSemaphore = new Semaphore(1);

    private void pauseDataIngestion() throws InterruptedException {
        dataFlowSemaphore.acquire();
    }

    private void resumeDataIngestion() {
        dataFlowSemaphore.release();
    }

    @Before
    public void setupIngestRuleAndImplementation() {
        execute(String.format(Locale.ENGLISH, "create table %s (data int)", TARGET_TABLE));
        execute(String.format(Locale.ENGLISH,
            "create ingest rule %s on %s where topic = ? into %s",
            UNDER_TEST_INGEST_RULE_NAME, INGESTION_SOURCE_ID, TARGET_TABLE),
            new Object[]{DATA_TOPIC}
        );
        implementation = new TestIngestionImplementation(internalCluster().getInstance(Functions.class),
            internalCluster().getInstance(IngestionService.class));
        implementation.registerIngestionImplementation();
        produceData = true;
        dataFlowingLatch = new CountDownLatch(1);
        dataIngestionThread = new Thread(() -> {
            while (produceData) {
                try {
                    dataFlowSemaphore.acquire();
                    Integer nextValue = dataSource.next();
                    implementation.ingestData(DATA_TOPIC, nextValue);
                    lastProducedData.set(nextValue);
                } catch (InterruptedException e) {
                    // will retry if interrupted
                } finally {
                    dataFlowSemaphore.release();
                }
                dataFlowingLatch.countDown();
            }
        }, "DataIngestionThread");
        dataIngestionThread.start();
    }

    @After
    public void dropIngestRuleAndStopProducingData() throws InterruptedException {
        execute("drop ingest rule if exists " + UNDER_TEST_INGEST_RULE_NAME);
        produceData = false;
        resumeDataIngestion();
        dataIngestionThread.join(5000);
    }

    @Test
    public void testIngestionImplementationIngestsData() throws Exception {
        dataFlowingLatch.await(5, TimeUnit.SECONDS);
        pauseDataIngestion();

        execute("select * from ingest_data_raw order by data desc");
        assertThat(response.rows()[0][0], is(lastProducedData.get()));
    }

    @Test
    public void testCreateRuleOnDifferentTopicThanIncomingData() throws Exception {
        execute("create table other_topic_raw_table (data int)");
        execute(String.format(Locale.ENGLISH,
            "create ingest rule rule_for_other_topic on %s where topic = '%s' into %s",
            INGESTION_SOURCE_ID, "other_topic", "other_topic_raw_table")
        );

        dataFlowingLatch.await(5, TimeUnit.SECONDS);
        execute("select * from other_topic_raw_table order by data desc");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testCreateMultipleRulesForTheSameTargetTable() throws Exception {
        execute(String.format(Locale.ENGLISH,
            "create ingest rule another_topic_rule on %s where topic = '%s' into %s",
            INGESTION_SOURCE_ID, DATA_TOPIC, TARGET_TABLE)
        );
        dataFlowingLatch.await(5, TimeUnit.SECONDS);
        pauseDataIngestion();

        // last produced data should've been ingested twice in the target table
        execute("select * from ingest_data_raw order by data desc limit 2");
        assertThat(response.rows()[0][0], is(lastProducedData.get()));
        assertThat(response.rows()[1][0], is(lastProducedData.get()));
    }

    @Test
    public void testDropIngestRuleWillCauseImplementationToStopIngesting() throws Exception {
        dataFlowingLatch.await(5, TimeUnit.SECONDS);

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
        assertThat(response.rows()[0][0], is(expectedLastIngestedValue));
    }
}
