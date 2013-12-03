package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@AxisRange(min = 0)
@BenchmarkMethodChart(filePrefix = "benchmark-groupby")
public class GroupByBenchmark extends BenchmarkBase {

    public ESLogger logger = Loggers.getLogger(getClass());
    public static final int NUMBER_OF_DOCUMENTS = 1000000;
    public static final int BENCHMARK_ROUNDS = 1000;

    public static SQLRequest maxRequest = new SQLRequest(String.format("select max(\"areaInSqKm\") from %s group by continent", INDEX_NAME));
    public static SQLRequest minRequest = new SQLRequest(String.format("select min(\"areaInSqKm\") from %s group by continent", INDEX_NAME));
    public static SQLRequest avgRequest = new SQLRequest(String.format("select avg(\"population\") from %s group by continent", INDEX_NAME));
    public static SQLRequest countStarRequest = new SQLRequest(String.format("select count(*) from %s group by continent", INDEX_NAME));
    public static SQLRequest countColumnRequest = new SQLRequest(String.format("select count(\"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest countDistinctRequest = new SQLRequest(String.format("select count(distinct \"countryName\") from %s group by continent", INDEX_NAME));
    public static SQLRequest anyRequest = new SQLRequest(String.format("select any(\"countryName\") from %s group by continent", INDEX_NAME));

    public static boolean dataGenerated = false;



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

    @Before
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
            refresh(client());
            dataGenerated = true;
            logger.info("{} documents generated.", NUMBER_OF_DOCUMENTS);
        }
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
        getClient(false).execute(SQLAction.INSTANCE, anyRequest).actionGet();
    }
}
