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

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-select")
public class SelectBenchmark extends BenchmarkBase {

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final int NUM_REQUESTS_PER_TEST = 10;
    public static final int BENCHMARK_ROUNDS = 100;
    public int apiGetRound = 0;
    public int sqlGetRound = 0;
    private List<String> someIds = new ArrayList<>(10);

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

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
    public void loadRandomId() {
        if (someIds.isEmpty()) {
            SQLRequestBuilder builder = new SQLRequestBuilder(client()).source(
                    new BytesArray("{\"stmt\":\"select \\\"_id\\\" from countries limit 10\"}")
            );
            SQLResponse response = client().execute(SQLAction.INSTANCE, builder.request()).actionGet();
            for (int i=0; i<response.rows().length; i++) {
                someIds.add((String)response.rows()[i][0]);
            }
        }
    }

    public String getGetId() {
        return someIds.get(getRandom().nextInt(someIds.size()));
    }


    public GetRequest getApiGetRequest() {
        return new GetRequest(INDEX_NAME, "default", getGetId());
    }

    public SQLRequest getSqlGetRequest() {
        return new SQLRequest(
            "SELECT * from " + INDEX_NAME + " WHERE \"_id\"=?",
            new Object[]{getGetId()}
        );
    }

    public SearchRequest getApiSearchRequest() {
        return new SearchRequest(new String[]{INDEX_NAME}, searchSource);
    }

    public SQLRequest getSqlSearchRequest() {
        return new SQLRequest(
            "SELECT * from " + INDEX_NAME + " WHERE \"countryCode\"=? or \"countryName\"='Micronesia'",
            new Object[]{"CU", "Micronesia"}
        );
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetApi() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            GetRequest request = getApiGetRequest();
            GetResponse response = client().execute(GetAction.INSTANCE, request).actionGet();
            assertTrue(String.format("Queried row '%s' does not exist (API). Round: %d", request.id(), apiGetRound), response.isExists());
            apiGetRound++;
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetSql() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLRequest request = getSqlGetRequest();
            SQLResponse response = client().execute(SQLAction.INSTANCE, request).actionGet();
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
    public void testSearchApi() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SearchResponse response = client().execute(SearchAction.INSTANCE, getApiSearchRequest()).actionGet();
            assertEquals(
                    "Did not find the two wanted rows (API).",
                    2L,
                    response.getHits().getTotalHits()
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSearchSql() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = client().execute(SQLAction.INSTANCE, getSqlSearchRequest()).actionGet();
            assertEquals(
                    "Did not find the two wanted rows (SQL).",
                    2,
                    response.rows().length
            );
        }
    }
}
