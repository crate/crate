/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-groupby-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-groupby")
public class GroupByBenchmark extends BenchmarkBase {

    public final static ESLogger logger = Loggers.getLogger(GroupByBenchmark.class);
    public static final int NUMBER_OF_DOCUMENTS = 500_000; // was 1_000_000
    public static final int BENCHMARK_ROUNDS = 1000;

    public static SQLRequest maxRequest = new SQLRequest(String.format("select max(\"areaInSqKm\") from %s group by continent", INDEX_NAME));
    public static SQLRequest minRequest = new SQLRequest(String.format("select min(\"areaInSqKm\") from %s group by continent", INDEX_NAME));
    public static SQLRequest avgRequest = new SQLRequest(String.format("select avg(\"population\") from %s group by continent", INDEX_NAME));
    public static SQLRequest countStarRequest = new SQLRequest(String.format("select count(*) from %s group by continent", INDEX_NAME));
    public static SQLRequest countColumnRequest = new SQLRequest(String.format("select count(\"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest countDistinctRequest = new SQLRequest(String.format("select count(distinct \"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest arbitraryRequest = new SQLRequest(String.format("select arbitrary(\"countryName\") from %s group by continent", INDEX_NAME));

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    protected boolean generateData() {
        return true;
    }

    protected byte[] generateRowSource() throws IOException {
        Random random = getRandom();
        byte[] buffer = new byte[32];
        random.nextBytes(buffer);
        return XContentFactory.jsonBuilder()
                .startObject()
                .field("areaInSqKm", random.nextFloat())
                .field("continent", new BytesArray(buffer, 0, 4).toUtf8())
                .field("countryCode", new BytesArray(buffer, 4, 8).toUtf8())
                .field("countryName", new BytesArray(buffer, 8, 24).toUtf8())
                .field("population", random.nextInt(Integer.MAX_VALUE))
                .endObject()
                .bytes().toBytes();
    }

    protected void doGenerateData() throws Exception {
        logger.info("generating {} documents...", NUMBER_OF_DOCUMENTS);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i=0; i<4; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    int numDocsToCreate = NUMBER_OF_DOCUMENTS/4;
                    logger.info("Generating {} Documents in Thread {}", numDocsToCreate, Thread.currentThread().getName());
                    Client client = getClient(false);
                    BulkRequest bulkRequest = new BulkRequest();

                    for (int i=0; i < numDocsToCreate; i+=1000) {
                        bulkRequest.requests().clear();
                        try {
                            byte[] source = generateRowSource();
                            for (int j=0; j<1000;j++) {
                                IndexRequest indexRequest = new IndexRequest(INDEX_NAME, "default", String.valueOf(i+j) + String.valueOf(Thread.currentThread().getId()));
                                indexRequest.source(source);
                                bulkRequest.add(indexRequest);
                            }
                            BulkResponse response = client.bulk(bulkRequest).actionGet();
                            assertFalse(response.hasFailures());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(2L, TimeUnit.MINUTES);
        executor.shutdownNow();
        getClient(true).admin().indices().prepareFlush(INDEX_NAME).setFull(true).execute().actionGet();
        getClient(false).admin().indices().prepareFlush(INDEX_NAME).setFull(true).execute().actionGet();
        refresh(client());
        logger.info("{} documents generated.", NUMBER_OF_DOCUMENTS);
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByMaxPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, maxRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByMinPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, minRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByAvgPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, avgRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountStarPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, countStarRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountColumnPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, countColumnRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByCountDistinctPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, countDistinctRequest).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 10)
    @Test
    public void testGroupByAnyPerformance() {
        getClient(false).execute(SQLAction.INSTANCE, arbitraryRequest).actionGet();
    }
}
