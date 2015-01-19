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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-update-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-update")
public class UpdateBenchmark extends BenchmarkBase {

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 100;

    public String updateId = null;
    public String updateIdqueryPlannerEnabled = null;

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    @Override
    public boolean loadData() {
        return true;
    }

    @Before
    public void getUpdateIds() {
        if (updateId == null || updateIdqueryPlannerEnabled == null) {
            SQLRequest request = new SQLRequest("SELECT \"_id\" FROM countries WHERE \"countryCode\"=?", new Object[]{"AT"});
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, request).actionGet();
            assert response.rows().length == 1;
            updateId = (String)response.rows()[0][0];

            request = new SQLRequest("SELECT \"_id\" FROM countries WHERE \"countryCode\"=?", new Object[]{"AT"});
            response = getClient(true).execute(SQLAction.INSTANCE, request).actionGet();
            assert response.rows().length == 1;
            updateIdqueryPlannerEnabled = (String)response.rows()[0][0];
        }
    }

    public SQLRequest getSqlUpdateByIdRequest(boolean queryPlannerEnabled) {
        return new SQLRequest("UPDATE countries SET population=? WHERE \"_id\"=?", new Object[]{ Math.abs(getRandom().nextInt()), queryPlannerEnabled ? updateIdqueryPlannerEnabled : updateId });
    }

    public UpdateRequest getApiUpdateByIdRequest(boolean queryPlannerEnabled) {
        Map<String, Integer> updateDoc = new HashMap<>();
        updateDoc.put("population", Math.abs(getRandom().nextInt()));
        return new UpdateRequest(INDEX_NAME, "default", queryPlannerEnabled ? updateIdqueryPlannerEnabled : updateId).doc(updateDoc);
    }

    public SQLRequest getSqlUpdateRequest() {
        return new SQLRequest("UPDATE countries SET population=? WHERE \"countryCode\"=?", new Object[]{ Math.abs(getRandom().nextInt()), "US" });
    }

    public SearchRequest getApiUpdateRequest() throws IOException {
        SearchRequest request = new SearchRequest(INDEX_NAME).types("default");
        request.source(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("query")
                        .startObject("term")
                        .field("countryCode", "US")
                        .endObject()
                        .endObject()
                        .startObject("facets")
                        .startObject("update")
                        .startObject("update")
                        .field("doc", new MapBuilder<String, Object>().put("population", Math.abs(getRandom().nextInt())).map())
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
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getSqlUpdateRequest()).actionGet();
            assertEquals(
                    1,
                    response.rowCount()
            );
        }
    }


    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testUpdateSqlById() {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            SQLResponse response = getClient(false).execute(SQLAction.INSTANCE, getSqlUpdateByIdRequest(false)).actionGet();
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
            UpdateResponse response = getClient(false).execute(UpdateAction.INSTANCE, getApiUpdateByIdRequest(false)).actionGet();
            assertEquals(updateId, response.getId());
        }
    }


}
