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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-select-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-select")
public class SelectBenchmark extends BenchmarkBase {

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 100;
    public int apiGetRound = 0;
    public int sqlGetRound = 0;
    private List<String> someIds = new ArrayList<>(10);
    private List<String> someIdsQueryPlannerEnabled = new ArrayList<>(10);

    public static final SQLRequest SYS_SHARDS_REQUEST = new SQLRequest("select * from sys.shards order by schema_name, table_name");

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    private static byte[] searchSource;

    @Override
    public boolean importData() {
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
            SQLRequestBuilder builder = new SQLRequestBuilder(client());
            builder.request().stmt(
                    "select \"_id\" from countries limit 10"
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

    public String getGetId(boolean queryPlannerEnabled, int idx) {
        List<String> l = queryPlannerEnabled ? someIdsQueryPlannerEnabled : someIds;
        return l.get(idx % l.size());
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
        return new SearchRequest(new String[]{INDEX_NAME}, searchSource).types("default");
    }

    public SQLRequest getSqlSearchRequest() {
        return new SQLRequest(
            "SELECT * from " + INDEX_NAME + " WHERE \"countryCode\" IN (?,?,?)",
            new Object[]{"CU", "KP", "RU"}
        );
    }

    public MultiGetRequest getMultiGetApiRequest() {
        MultiGetRequest request = new MultiGetRequest();
        for (int i = 0; i<3;i++) {
            request.add(
                    new MultiGetRequest.Item(INDEX_NAME, "default", getGetId(false, i))
            );
        }
        return request;
    }

    public SQLRequest getMultiGetSqlRequest(boolean queryPlannerEnabled) {

        return new SQLRequest(
                "SELECT * FROM " + INDEX_NAME + " WHERE \"_id\"=? OR \"_id\"=? OR \"_id\"=?",
                new Object[]{getGetId(queryPlannerEnabled, 0), getGetId(queryPlannerEnabled, 1), getGetId(queryPlannerEnabled, 2) }
        );
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetSingleResultApi() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            GetRequest request = getApiGetRequest(false);
            GetResponse response = getClient(false).execute(GetAction.INSTANCE, request).actionGet();
            assertTrue(String.format(Locale.ENGLISH, "Queried row '%s' does not exist (API). Round: %d", request.id(), apiGetRound), response.isExists());
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
                    String.format(Locale.ENGLISH, "Queried row '%s' does not exist (SQL). Round: %d", request.args()[0], sqlGetRound),
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
                    "Did not find the three wanted rows (SQL).",
                    3,
                    response.rows().length
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetMultiGetApi() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            MultiGetResponse response = getClient(false).execute(MultiGetAction.INSTANCE, getMultiGetApiRequest()).actionGet();
            assertEquals(
                    "Did not find the three wanted rows (API, MultiGet)",
                    3,
                    response.getResponses().length
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testGetMultiGetSql() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getMultiGetSqlRequest(false)).actionGet();
            assertEquals(
                    "Did not find the three wanted rows (SQL, MultiGet)",
                    3,
                    response.rowCount()
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testSelectSysShardsBenchmark() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            getClient(false).execute(SQLAction.INSTANCE, SYS_SHARDS_REQUEST).actionGet();
        }

    }
}
