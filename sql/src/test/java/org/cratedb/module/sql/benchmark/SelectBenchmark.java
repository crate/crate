package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLRequestBuilder;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@AxisRange(min = 0)
@BenchmarkMethodChart(filePrefix = "benchmark-select")
public class SelectBenchmark extends BenchmarkBase {

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 100;
    public int apiGetRound = 0;
    public int sqlGetRound = 0;
    private List<String> someIds = new ArrayList<>(10);
    private List<String> someIdsQueryPlannerEnabled = new ArrayList<>(10);

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    private static byte[] searchSource;

    @Override
    public boolean loadData() {
        return true;
    }

    @BeforeClass
    public static void generateSearchSource() throws IOException {
        searchSource = XContentFactory.jsonBuilder()
                .startObject()
                    .array("fields", "areaInSqKm", "captial", "continent", "continentName", "countryCode", "countryName", "north", "east", "south", "west", "fipsCode", "currencyCode", "languages", "isoAlpha3", "isoNumeric", "population")
                    .startObject("query")
                        .startObject("bool")
                            .field("minimum_should_match", 1)
                            .startArray("should")
                                .startObject()
                                    .startObject("term")
                                    .field("countryCode", "CU")
                                    .endObject()
                                .endObject()
                                .startObject()
                                    .startObject("term")
                                    .field("countryName", "Micronesia")
                                    .endObject()
                                .endObject()
                            .endArray()
                        .endObject()
                    .endObject()
                .endObject().bytes().toBytes();
    }

    @Before
    public void loadRandomIds() {
        if (someIds.isEmpty()) {
            SQLRequestBuilder builder = new SQLRequestBuilder(client()).source(
                    new BytesArray("{\"stmt\":\"select \\\"_id\\\" from countries limit 10\"}")
            );
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, builder.request()).actionGet();
            for (int i=0; i<response.rows().length; i++) {
                someIds.add((String)response.rows()[i][0]);
            }

            response = getClient(true).execute(SQLAction.INSTANCE, builder.request()).actionGet();
            for (int i=0; i<response.rows().length; i++) {
                someIdsQueryPlannerEnabled.add((String)response.rows()[i][0]);
            }

        }
    }

    public String getGetId(boolean queryPlannerEnabled) {
        List<String> l = queryPlannerEnabled ? someIdsQueryPlannerEnabled : someIds;
        return l.get(getRandom().nextInt(l.size()));
    }


    public GetRequest getApiGetRequest(boolean queryPlannerEnabled) {
        return new GetRequest(INDEX_NAME, "default", getGetId(queryPlannerEnabled));
    }

    public SQLRequest getSqlGetRequest(boolean queryPlannerEnabled) {
        return new SQLRequest(
            "SELECT * from " + INDEX_NAME + " WHERE \"_id\"=?",
            new Object[]{getGetId(queryPlannerEnabled)}
        );
    }

    public SearchRequest getApiSearchRequest() {
        return new SearchRequest(new String[]{INDEX_NAME}, searchSource);
    }

    public SQLRequest getSqlSearchRequest() {
        return new SQLRequest(
            "SELECT * from " + INDEX_NAME + " WHERE \"countryCode\" IN (?,?,?)",
            new Object[]{"CU", "KP", "RU"}
        );
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetSingleResultApi() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            GetRequest request = getApiGetRequest(false);
            GetResponse response = getClient(false).execute(GetAction.INSTANCE, request).actionGet();
            assertTrue(String.format("Queried row '%s' does not exist (API). Round: %d", request.id(), apiGetRound), response.isExists());
            apiGetRound++;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetSingleResultSql() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLRequest request = getSqlGetRequest(false);
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, request).actionGet();
            assertEquals(
                    String.format("Queried row '%s' does not exist (SQL). Round: %d", request.args()[0], sqlGetRound),
                    1,
                    response.rows().length
            );
            sqlGetRound++;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetSingleResultSqlQueryPlannerEnabled() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLRequest request = getSqlGetRequest(true);
            SQLResponse response = getClient(true).execute(SQLAction.INSTANCE, request).actionGet();
            assertEquals(
                    String.format("Queried row '%s' does not exist (SQL). Round: %d", request.args()[0], sqlGetRound),
                    1,
                    response.rows().length
            );
            sqlGetRound++;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetMultipleResultsApi() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SearchResponse response = getClient(false).execute(SearchAction.INSTANCE, getApiSearchRequest()).actionGet();
            assertEquals(
                    "Did not find the two wanted rows (API).",
                    2L,
                    response.getHits().getTotalHits()
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetMultipleResultsSql() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getSqlSearchRequest()).actionGet();
            assertEquals(
                    "Did not find the two wanted rows (SQL).",
                    3,
                    response.rows().length
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetMultipleResultsSqlQueryPlannerEnabled() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getSqlSearchRequest()).actionGet();
            assertEquals(
                    "Did not find the two wanted rows (SQL).",
                    3,
                    response.rows().length
            );
        }
    }
}
