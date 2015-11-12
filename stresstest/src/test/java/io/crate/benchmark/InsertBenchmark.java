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
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-insert-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-insert")
public class InsertBenchmark extends BenchmarkBase {

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 100;
    public static final String SINGLE_INSERT_SQL_STMT = "INSERT INTO countries " +
            "(\"countryName\", \"countryCode\", \"isoNumeric\", \"east\", \"north\", \"west\", \"south\"," +
            "\"isoAlpha3\", \"currencyCode\", \"continent\", \"continentName\", \"languages\", \"fipsCode\", \"capital\", \"population\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    public static final String BULK_INSERT_SQL_STMT = "INSERT INTO countries " +
            "(\"countryName\", \"countryCode\", \"isoNumeric\", \"east\", \"north\", \"west\", \"south\"," +
            "\"isoAlpha3\", \"currencyCode\", \"continent\", \"continentName\", \"languages\", \"fipsCode\", \"capital\", \"population\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    public static String singleApiInsertSource;

    @BeforeClass
    public static void prepareIndexSource() throws IOException {
        singleApiInsertSource = XContentFactory.jsonBuilder().startObject()
                .field("countryName", "Mordor")
                .field("countryCode", "MO")
                .field("isoNumeric", "666")
                .field("east", 0.0)
                .field("north", 180.0)
                .field("west", 90.0)
                .field("south", 0.0)
                .field("isoAlpha3", "MOR")
                .field("currencyCode", "NAZ")
                .field("continent", "ME")
                .field("continentName", "Mittelerde")
                .field("languages", "naz")
                .field("fipsCode", "MOR")
                .field("capital", "Schicksalsberg")
                .field("population", 1000)
                .endObject().string();
    }

    private IndexRequest getSingleApiInsertRequest() {
        IndexRequest request = new IndexRequest("countries", "default");
        request.create(true);
        request.source(singleApiInsertSource);
        return request;
    }

    private BulkRequest getBulkApiInsertRequest() {
        BulkRequest request = new BulkRequest();
        for (int i=0; i<4;i++) {
            request.add(
                getSingleApiInsertRequest()
            );
        }
        return request;
    }

    @BenchmarkOptions(benchmarkRounds =  BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testInsertSingleSql() {
        for (int i=0;i<NUM_REQUESTS_PER_TEST;i++) {
            execute(SINGLE_INSERT_SQL_STMT,
                    new Object[]{"Mordor", "MO", "666", 0.0, 180.0, 90.0, 0.0,
                            "MOR", "NAZ", "ME", "Mittelerde", "naz", "MOR",
                            "Schicksalsberg", 1000 });
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testInsertBulkSql() {
        for (int i=0;i<NUM_REQUESTS_PER_TEST;i++) {
            execute(BULK_INSERT_SQL_STMT,
                    new Object[]{
                            "Mordor", "MO", "666", 0.0, 180.0, 90.0, 0.0,
                            "MOR", "NAZ", "ME", "Mittelerde", "naz", "MOR",
                            "Schicksalsberg", 1000,

                            "Auenland", "AU", "123", 1.1, 2.2, 3.3, 4.4,
                            "AUL", "BOC", "ME", "Mittelerde", "boc", "AUL",
                            "Hobbingen", 200,

                            "Mordor", "MO", "666", 0.0, 180.0, 90.0, 0.0,
                            "MOR", "NAZ", "ME", "Mittelerde", "naz", "MOR",
                            "Schicksalsberg", 1000,

                            "Auenland", "AU", "123", 1.1, 2.2, 3.3, 4.4,
                            "AUL", "BOC", "ME", "Mittelerde", "boc", "AUL",
                            "Hobbingen", 200
                    });
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testInsertSingleApi() {
        for (int i=0;i<NUM_REQUESTS_PER_TEST;i++) {
            client().execute(IndexAction.INSTANCE, getSingleApiInsertRequest()).actionGet();
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testInsertBulkApi() {
        for (int i=0;i<NUM_REQUESTS_PER_TEST;i++) {
            client().execute(BulkAction.INSTANCE, getBulkApiInsertRequest()).actionGet();
        }
    }
}
