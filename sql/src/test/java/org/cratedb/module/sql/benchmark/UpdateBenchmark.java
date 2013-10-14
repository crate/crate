package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-update")
public class UpdateBenchmark extends BenchmarkBase {

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 100;

    public String updateId;

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Override
    public boolean loadData() {
        return true;
    }

    @Before
    public void getUpdateId() {
        if (updateId == null) {
            SQLRequest request = new SQLRequest("SELECT \"_id\" FROM countries WHERE \"countryCode\"=?", new Object[]{"AT"});
            SQLResponse response = client().execute(SQLAction.INSTANCE, request).actionGet();
            assert response.rows().length == 1;
            updateId = (String)response.rows()[0][0];
        }
    }

    public SQLRequest getSqlUpdateByIdRequest() {
        return new SQLRequest("UPDATE countries SET population=? WHERE \"_id\"=?", new Object[]{ Math.abs(getRandom().nextInt()), updateId });
    }

    public UpdateRequest getApiUpdateByIdRequest() {
        Map<String, Integer> updateDoc = new HashMap<>();
        updateDoc.put("population", Math.abs(getRandom().nextInt()));
        return new UpdateRequest(INDEX_NAME, "default", updateId).doc(updateDoc);
    }

    public SQLRequest getSqlUpdateRequest() {
        return new SQLRequest("UPDATE countries SET population=? WHERE \"countryCode\"=?", new Object[]{ Math.abs(getRandom().nextInt()), "US" });
    }

    public SearchRequest getApiUpdateRequest() throws IOException {
        SearchRequest request = new SearchRequest(INDEX_NAME);
        request.source(
                XContentFactory.jsonBuilder()
                        .startObject()
                            .startObject("query")
                                .startObject("term")
                                    .field("countryCode", "US")
                                .endObject()
                            .endObject()
                            .startObject("facets")
                                .startObject("sql")
                                    .startObject("sql")
                                        .field("stmt", "UPDATE countries SET population=? WHERE \"countryCode\"=?")
                                        .field("args", new Object[]{Math.abs(getRandom().nextInt()), updateId})
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject().bytes().toBytes()
        );
        return request;
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpdateSql() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = client().execute(SQLAction.INSTANCE, getSqlUpdateRequest()).actionGet();
            assertEquals(
                    1,
                    response.rowCount()
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpdateApi() throws IOException {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SearchResponse response = client().execute(SearchAction.INSTANCE, getApiUpdateRequest()).actionGet();
            assertEquals(1, response.getHits().totalHits());
        }
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpdateSqlById() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = client().execute(SQLAction.INSTANCE, getSqlUpdateByIdRequest()).actionGet();
            assertEquals(
                    1,
                    response.rowCount()
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpdateApiById() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            UpdateResponse response = client().execute(UpdateAction.INSTANCE, getApiUpdateByIdRequest()).actionGet();
            assertEquals(updateId, response.getId());
        }
    }
}
