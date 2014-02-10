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

package org.cratedb.module.sql.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;

@AxisRange(min = 0)
@BenchmarkMethodChart(filePrefix = "benchmark-delete")
public class DeleteBenchmark extends BenchmarkBase {

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 24; // Don't exceed the number of deletable rows
    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    private List<String> ids = new ArrayList<>(250);
    private List<String> countryCodes = new ArrayList<>(250);

    @Override
    public boolean loadData() {
        return true;
    }

    @Before
    public void prepare() throws Exception {
        if (ids.isEmpty() || countryCodes.isEmpty()) {
            doLoadData();
            // setupOnce non-static
            SQLRequest request = new SQLRequest("SELECT \"_id\", \"countryCode\" FROM countries");
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, request).actionGet();
            for (int i=0; i<response.rows().length;i++ ) {
                ids.add((String) response.rows()[i][0]);
                countryCodes.add((String) response.rows()[i][1]);
            }
        }
    }

    public String getDeleteId() throws Exception {
        if (ids.isEmpty()) {
           prepare();
        }
        return ids.remove(0);
    }

    public String getCountryCode() throws Exception {
        if (countryCodes.isEmpty()) {
            prepare();
        }
        return countryCodes.remove(0);
    }

    public DeleteRequest getDeleteApiByIdRequest() throws Exception {
        return new DeleteRequest(INDEX_NAME, "default", getDeleteId());
    }

    public SQLRequest getDeleteSqlByIdRequest() throws Exception {
        return new SQLRequest("DELETE FROM countries WHERE \"_id\"=?", new Object[]{ getDeleteId() });
    }

    public SQLRequest getDeleteSqlByQueryRequest() throws Exception {
        return new SQLRequest("DELETE FROM countries WHERE \"countryCode\"=?", new Object[]{ getCountryCode() });
    }

    public DeleteByQueryRequest getDeleteApiByQueryRequest() throws Exception {

        return new DeleteByQueryRequest(INDEX_NAME).query(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("term")
                        .field("countryCode", getCountryCode())
                        .endObject()
                        .endObject().bytes().toBytes()
        );
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteApiById() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            DeleteResponse response = getClient(false).execute(DeleteAction.INSTANCE, getDeleteApiByIdRequest()).actionGet();
            assertFalse(response.isNotFound());
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteApiByQuery() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            getClient(false).execute(DeleteByQueryAction.INSTANCE, getDeleteApiByQueryRequest()).actionGet();
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteSqlById() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getDeleteSqlByIdRequest()).actionGet();
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteSqlByIdQueryPlannerEnabled() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(true).execute(SQLAction.INSTANCE, getDeleteSqlByIdRequest()).actionGet();
        }
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteSQLByQuery() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getDeleteSqlByQueryRequest()).actionGet();
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteSQLByQueryQueryPlannerEnabled() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(true).execute(SQLAction.INSTANCE, getDeleteSqlByQueryRequest()).actionGet();
        }
    }
}
