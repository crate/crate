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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-like-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-like")
public class LikeBenchmark extends BenchmarkBase {

    static final int BENCHMARK_ROUNDS = 200;
    static final int QUERIES = 100;
    static final int NUMBER_OF_DOCUMENTS = 100_000;

    // must be in-sync with core->Constants.DEFAULT_MAPPING_TYPE
    static final String DEFAULT_MAPPING_TYPE = "default";

    static AtomicInteger ID_VALUE = new AtomicInteger(0);

    @Override
    public boolean generateData() {
        return true;
    }

    @Override
    protected int numberOfDocuments() {
        return NUMBER_OF_DOCUMENTS;
    }

    @Override
    protected void createTable() {
        execute("create table " + INDEX_NAME + " (" +
                "  id int primary key, " +
                "  value string" +
                ") with (number_of_replicas=0)");
        client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForGreenStatus().execute().actionGet();

    }

    @Override
    protected byte[] generateRowSource() throws IOException {
        int value = ID_VALUE.getAndIncrement();
        String strValue;

        if (value % 1000 == 0) {
            strValue = String.format("%d XXX %d", value, value);
        } else {
            strValue = String.format("%d", value);
        }

        return XContentFactory.jsonBuilder()
                .startObject()
                .field("id", value)
                .field("value", strValue)
                .endObject()
                .bytes().toBytes();
    }

    protected boolean generateNewRowForEveryDocument() {
        return true;
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testLikePrefix() throws Exception {
        for (int i = 0; i < QUERIES; i++) {
            assertThat(
                    execute("select value from " + INDEX_NAME + " where value like '%XXX%' limit 10").rowCount(),
                    is(10L)
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testEsRegexpQuery() throws Exception {
        for (int i = 0; i < QUERIES; i++) {
            assertThat(
                client().prepareSearch(INDEX_NAME)
                        .setTypes(DEFAULT_MAPPING_TYPE)
                        .setQuery("{\"regexp\":{\"value\":\".*XXX.*\"}}")
                        .addField("value")
                        .setSize(10)
                        .execute().actionGet().getHits().getHits().length,
                is(10)
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testEsRegexpFilter() throws Exception {
        for (int i = 0; i < QUERIES; i++) {
            assertThat(
                    client().prepareSearch(INDEX_NAME)
                            .setTypes(DEFAULT_MAPPING_TYPE)
                            .setQuery("{\"filtered\":{\"query\":{\"match_all\":{}}, \"filter\":{\"regexp\":{\"value\":\".*XXX.*\"}}}}")
                            .addField("value")
                            .setSize(10)
                            .execute().actionGet().getHits().getHits().length,
                    is(10)
            );
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testEsWildcard() throws Exception {
        for (int i = 0; i < QUERIES; i++) {
            assertThat(client().prepareSearch(INDEX_NAME)
                    .setTypes(DEFAULT_MAPPING_TYPE)
                    .setQuery("{\"wildcard\":{\"value\":\"*XXX*\"}}")
                    .addField("value")
                    .setSize(10)
                    .execute().actionGet().getHits().getHits().length, is(10));
        }
    }
}
