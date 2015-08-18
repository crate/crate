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

package io.crate.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.Paging;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.SortingTopNProjector;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.CollectingProjector;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.sort.SortBuilders;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@BenchmarkHistoryChart(filePrefix="benchmark-lucenedoccollector-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-lucenedoccollector")
public class LuceneDocCollectorBenchmark extends BenchmarkBase {

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static boolean dataGenerated = false;
    public static final int NUMBER_OF_DOCUMENTS = 100_000;
    public static final int BENCHMARK_ROUNDS = 100;
    public static final int WARMUP_ROUNDS = 10;

    public static final int PAGE_SIZE = 10_000;

    public final static ESLogger logger = Loggers.getLogger(LuceneDocCollectorBenchmark.class);

    private ShardId shardId = new ShardId(INDEX_NAME, 0);
    private JobContextService jobContextService;
    private ShardCollectService shardCollectService;
    private OrderBy orderBy;
    private CollectingProjector collectingProjector = new CollectingProjector();
    private Reference reference;

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));
    private JobCollectContext jobCollectContext;

    @Override
    public byte[] generateRowSource() throws IOException {
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

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        IndexService indexService;
        try {
            IndicesService instanceFromNode = CLUSTER.getInstance(IndicesService.class, NODE2);
            indexService = instanceFromNode.indexServiceSafe(INDEX_NAME);
        } catch (IndexMissingException e) {
            IndicesService instanceFromNode = CLUSTER.getInstance(IndicesService.class, NODE1);
            indexService = instanceFromNode.indexServiceSafe(INDEX_NAME);
        }

        shardCollectService = indexService.shardInjectorSafe(0).getInstance(ShardCollectService.class);
        jobContextService = indexService.shardInjectorSafe(0).getInstance(JobContextService.class);

        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "continent");
        reference = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        orderBy = new OrderBy(ImmutableList.of((Symbol) reference), new boolean[]{false}, new Boolean[]{false});
    }

    @Override
    protected void createTable() {
        execute("create table \"" + INDEX_NAME + "\" (" +
                " \"areaInSqKm\" float," +
                " capital string," +
                " continent string," +
                " \"continentName\" string," +
                " \"countryCode\" string," +
                " \"countryName\" string," +
                " north float," +
                " east float," +
                " south float," +
                " west float," +
                " \"fipsCode\" string," +
                " \"currencyCode\" string," +
                " languages string," +
                " \"isoAlpha3\" string," +
                " \"isoNumeric\" string," +
                " population integer" +
                ") clustered into 1 shards with (number_of_replicas=0)", new Object[0], true);
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, List<Symbol> input) throws Exception{
        return createDocCollector(orderBy, limit, collectingProjector, input);
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, Projector projector, List<Symbol> input) throws Exception{
        UUID jobId = UUID.randomUUID();
        CollectPhase node = new CollectPhase(jobId, 0, "collect", null, input, ImmutableList.<Projection>of());
        node.whereClause(WhereClause.MATCH_ALL);
        node.orderBy(orderBy);
        node.limit(limit);
        node.whereClause(WhereClause.MATCH_ALL);
        node.maxRowGranularity(RowGranularity.DOC);

        ShardProjectorChain projectorChain = Mockito.mock(ShardProjectorChain.class);
        Mockito.when(projectorChain.newShardDownstreamProjector(Matchers.any(ProjectionToProjectorVisitor.class))).thenReturn(projector);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
        jobCollectContext = new JobCollectContext(jobId, node,
                CLUSTER.getInstance(MapSideDataCollectOperation.class),
                RAM_ACCOUNTING_CONTEXT, collectingProjector);
        builder.addSubContext(node.executionPhaseId(), jobCollectContext);
        LuceneDocCollector collector = (LuceneDocCollector)shardCollectService.getCollector(
                node, projectorChain, jobCollectContext, 0, PAGE_SIZE);
        return collector;
    }

    @Override
    protected void doGenerateData() throws Exception {
        if (!dataGenerated) {

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
            getClient(true).admin().indices().prepareFlush(INDEX_NAME).execute().actionGet();
            refresh(client());
            dataGenerated = true;
            logger.info("{} documents generated.", NUMBER_OF_DOCUMENTS);
        }
    }



    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorOrderedWithScrollingPerformance() throws Exception{
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderBy.orderBySymbols());
        docCollector.doCollect();
        collectingProjector.finish();
        MatcherAssert.assertThat(collectingProjector.rows.size(), CoreMatchers.is(NUMBER_OF_DOCUMENTS));
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorOrderedWithoutScrollingPerformance() throws Exception{
        LuceneDocCollector docCollector = createDocCollector(orderBy, NUMBER_OF_DOCUMENTS, orderBy.orderBySymbols());
        docCollector.doCollect();
        collectingProjector.finish();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorUnorderedWithTopNProjection() throws Exception{
       InputCollectExpression expr = new InputCollectExpression(0);
       SortingTopNProjector topNProjector = new SortingTopNProjector(
                new Input[]{expr},
                new CollectExpression[]{expr},
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{false},
                NUMBER_OF_DOCUMENTS,
                0
        );
        topNProjector.downstream(collectingProjector);
        topNProjector.startProjection(jobCollectContext);
        LuceneDocCollector docCollector = createDocCollector(null, null, topNProjector, ImmutableList.of((Symbol) reference));
        docCollector.doCollect();
        topNProjector.doFinish();
        collectingProjector.finish();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorUnorderedPerformance() throws Exception{
        LuceneDocCollector docCollector = createDocCollector(null, null, ImmutableList.of((Symbol) reference));
        docCollector.doCollect();
        collectingProjector.finish();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testElasticsearchOrderedWithScrollingPerformance() throws Exception{
        int totalHits = 0;
        SearchResponse response = getClient(true).prepareSearch(INDEX_NAME).setTypes("default")
                                    .addField("continent")
                                    .addSort(SortBuilders.fieldSort("continent").missing("_last"))
                                    .setScroll("1m")
                                    .setSize(Paging.PAGE_SIZE)
                                    .execute().actionGet();
        totalHits += response.getHits().hits().length;
        while ( totalHits < NUMBER_OF_DOCUMENTS) {
            response = getClient(true).prepareSearchScroll(response.getScrollId()).setScroll("1m").execute().actionGet();
            totalHits += response.getHits().hits().length;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testElasticsearchOrderedWithoutScrollingPerformance() throws Exception{
        getClient(true).prepareSearch(INDEX_NAME).setTypes("default")
                .addField("continent")
                .addSort(SortBuilders.fieldSort("continent").missing("_last"))
                .setSize(NUMBER_OF_DOCUMENTS)
                .execute().actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testElasticsearchUnorderedWithoutScrollingPerformance() throws Exception{
        getClient(true).prepareSearch(INDEX_NAME).setTypes("default")
                .addField("continent")
                .setSize(NUMBER_OF_DOCUMENTS)
                .execute().actionGet();
    }
}
