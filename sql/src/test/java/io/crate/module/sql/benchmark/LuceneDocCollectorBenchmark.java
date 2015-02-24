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
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.collect.LuceneDocCollector;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.SortingTopNProjector;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.junit.Assert.assertFalse;

@BenchmarkHistoryChart(filePrefix="benchmark-lucenedoccollector-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-lucenedoccollector")
public class LuceneDocCollectorBenchmark extends BenchmarkBase {

    public static boolean dataGenerated = false;
    public static final int NUMBER_OF_DOCUMENTS = 100_000;
    public static final int BENCHMARK_ROUNDS = 100;
    public static final int WARMUP_ROUNDS = 10;


    public final static ESLogger logger = Loggers.getLogger(LuceneDocCollectorBenchmark.class);

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ShardId shardId = new ShardId(INDEX_NAME, 0);
    private LuceneQueryBuilder builder;
    private IndexService indexService;
    private CacheRecycler cacheRecycler;
    private PageCacheRecycler pageCacheRecycler;
    private BigArrays bigArrays;
    private Functions functions;
    private OrderBy orderBy;

    private CollectingProjector collectingProjector = new CollectingProjector();
    private OrderByCollectorExpression orderByCollectorExpr;
    private LuceneCollectorExpression luceneCollectorExpr;

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));



    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    public boolean loadData() {
        return false;
    }

    private byte[] generateRowSource() throws IOException {
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
    public Client getClient(boolean firstNode) {
        return cluster.client(NODE1);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        threadPool = new ThreadPool(getClass().getSimpleName());
        if (NODE1 == null) {
            NODE1 = cluster.startNode(getNodeSettings(1));
        }
        if (!indexExists()) {
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
            refresh(client());
            generateData();
        }


        clusterService = cluster.getInstance(ClusterService.class);
        functions = new ModulesBuilder()
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
        builder = new LuceneQueryBuilder(functions);
        IndicesService instanceFromNode = cluster.getInstanceFromNode(IndicesService.class);
        indexService = instanceFromNode.indexServiceSafe(INDEX_NAME);
        bigArrays = indexService.injector().getInstance(BigArrays.class);
        cacheRecycler = indexService.injector().getInstance(CacheRecycler.class);
        pageCacheRecycler = indexService.injector().getInstance(PageCacheRecycler.class);
        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "continent");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        orderBy = new OrderBy(ImmutableList.of((Symbol)ref), new boolean[]{false}, new Boolean[]{false});
        orderByCollectorExpr = new OrderByCollectorExpression(ref, orderBy);
        luceneCollectorExpr = LuceneDocLevelReferenceResolver.INSTANCE.getImplementation(ref.info());
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, LuceneCollectorExpression expression) throws Exception{
        return createDocCollector(orderBy, limit, expression, collectingProjector);
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, LuceneCollectorExpression expression, RowDownstream downstreamProjector) throws Exception{
        CollectNode node = new CollectNode();
        node.whereClause(WhereClause.MATCH_ALL);
        node.orderBy(orderBy);
        if(limit != null) {
            node.limit(limit);
        }
        node.whereClause(WhereClause.MATCH_ALL);
        return new LuceneDocCollector(threadPool,
                clusterService,
                builder,
                shardId,
                indexService,
                null,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                (List)ImmutableList.<Input>of(expression),
                (List)ImmutableList.of(expression),
                functions,
                node,
                downstreamProjector);
    }

    public void generateData() throws Exception {
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
            getClient(true).admin().indices().prepareFlush(INDEX_NAME).setFull(true).execute().actionGet();
            refresh(client());
            dataGenerated = true;
            logger.info("{} documents generated.", NUMBER_OF_DOCUMENTS);
        }
    }



    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorOrderedWithScrollingPerformance() throws Exception{
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderByCollectorExpr);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        collectingProjector.finish();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorOrderedWithoutScrollingPerformance() throws Exception{
        LuceneDocCollector docCollector = createDocCollector(orderBy, NUMBER_OF_DOCUMENTS, orderByCollectorExpr);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
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
        topNProjector.startProjection();
        LuceneDocCollector docCollector = createDocCollector(null, null, luceneCollectorExpr, topNProjector);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        topNProjector.finishProjection();
        collectingProjector.finish();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testLuceneDocCollectorUnorderedPerformance() throws Exception{
        LuceneDocCollector docCollector = createDocCollector(null, null, luceneCollectorExpr);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        collectingProjector.finish();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = WARMUP_ROUNDS)
    @Test
    public void testElasticsearchOrderedWithScrollingPerformance() throws Exception{
        int totalHits = 0;
        SearchResponse response = getClient(true).prepareSearch(INDEX_NAME).setTypes("default")
                                    .addField("continent")
                                    .addSort(fieldSort("continent").missing("_last"))
                                    .setScroll("1m")
                                    .setSize(LuceneDocCollector.PAGE_SIZE)
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
                .addSort(fieldSort("continent").missing("_last"))
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

    @After
    public void cleanUp() {
        threadPool.shutdownNow();
    }
}
