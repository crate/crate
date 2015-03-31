/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-select-limit-order-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-select-limit-order")
public class SelectLimitOrderBenchmark extends BenchmarkBase {

    public static final int BENCHMARK_ROUNDS = 10;
    public static final int NUMBER_OF_DOCUMENTS = 1_200_000;
    public static final String INDEX_NAME = "bench_select_limit_order";

    private String[] generatedStrings;

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    protected String tableName() {
        return INDEX_NAME;
    }

    @Override
    protected void createTable() {
        execute("create table " + INDEX_NAME +
                " (str_val string, int_val int) " +
                "clustered into 4 shards with (number_of_replicas=0)");
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();

        generatedStrings = generateRandomStrings(NUMBER_OF_DOCUMENTS * 0.10);
    }

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    protected int numberOfDocuments() {
        return NUMBER_OF_DOCUMENTS;
    }

    @Override
    protected byte[] generateRowSource() throws IOException {
        Random random = getRandom();
        return XContentFactory.jsonBuilder()
                .startObject()
                .field("str_val", generatedStrings[random.nextInt(generatedStrings.length)])
                .field("int_val", random.nextInt(Integer.MAX_VALUE))
                .endObject()
                .bytes().toBytes();
    }


    protected String[] generateRandomStrings(Number amount) {
        String[] strings = new String[amount.intValue()];
        for (int i = 0; i < amount.intValue(); i++) {
            strings[i] = RandomStrings.randomRealisticUnicodeOfCodepointLengthBetween(getRandom(), 1, 100);
        }
        return strings;
    }


    protected SearchRequest getApiSearchRequest(Integer limit, Integer offset, boolean orderBy) throws IOException {
        XContentBuilder builder =  XContentFactory.jsonBuilder()
                .startObject()
                    .field("from", offset)
                    .field("size", limit);

        if (orderBy) {
            builder.array("sort", "str_val", "int_val");
        }
        builder.startObject("query")
                    .startObject("match_all")
                .endObject()
                .endObject();
        return new SearchRequest(new String[]{INDEX_NAME}, builder.bytes().toBytes()).types("default");
    }

    protected SQLRequest getSqlSearchRequest(Integer limit, Integer offset, boolean orderBy) {
        String stmt = "SELECT * FROM " + INDEX_NAME;
        if (orderBy) {
            stmt += " ORDER BY str_val, int_val";
        }
        stmt += " LIMIT " + limit + " OFFSET " + offset;
        return new SQLRequest(stmt);
    }

    protected void runESBenchmark(int numRequest, Integer limit, Integer offset, boolean orderBy) throws Exception {
        for (int i=0; i<numRequest; i++) {
            SearchResponse response = getClient(false).execute(SearchAction.INSTANCE, getApiSearchRequest(limit, offset, orderBy)).actionGet();
            assertEquals(
                    "Did not get all wanted rows (ES)",
                    limit.longValue(),
                    response.getHits().hits().length
            );
        }
    }

    protected void runSQLBenchmark(int numRequest, Integer limit, Integer offset, boolean orderBy) throws Exception {
        for (int i=0; i<numRequest; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getSqlSearchRequest(limit, offset, orderBy)).actionGet();
            assertEquals(
                    "Did not get all wanted rows (SQL)",
                    limit.longValue(),
                    response.rowCount()
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void benchLimit1000000_ES() throws Exception {
        Integer limit = 1_000_000;
        Integer offset = 0;
        boolean orderBy = false;
        runESBenchmark(1, limit, offset, orderBy);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void benchLimit1000000_SQL() throws Exception {
        Integer limit = 1_000_000;
        Integer offset = 0;
        boolean orderBy = false;
        runSQLBenchmark(1, limit, offset, orderBy);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void benchLimit1000000Order_ES() throws Exception {
        Integer limit = 1_000_000;
        Integer offset = 0;
        boolean orderBy = true;
        runESBenchmark(1, limit, offset, orderBy);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void benchLimit1000000Order_SQL() throws Exception {
        Integer limit = 1_000_000;
        Integer offset = 0;
        boolean orderBy = true;
        runSQLBenchmark(1, limit, offset, orderBy);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void benchLimit100Offset100000Order_ES() throws Exception {
        Integer limit = 100;
        Integer offset = 100_000;
        boolean orderBy = true;
        runESBenchmark(1, limit, offset, orderBy);
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void benchLimit100Offset100000Order_SQL() throws Exception {
        Integer limit = 100;
        Integer offset = 100_000;
        boolean orderBy = true;
        runSQLBenchmark(1, limit, offset, orderBy);
    }
}
