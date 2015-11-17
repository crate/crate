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
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import io.crate.action.sql.SQLResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-delete-history", labelWith = LabelType.CUSTOM_KEY)
@BenchmarkMethodChart(filePrefix = "benchmark-delete")
public class DeleteBenchmark extends BenchmarkBase {

    public static final int NUM_REQUESTS_PER_TEST = 100;
    public static final int BENCHMARK_ROUNDS = 24; // Don't exceed the number of deletable rows

    private static final String DELETE_BY_ID_STMT = "DELETE FROM countries WHERE \"_id\" = ?";
    private static final String DELETE_BY_QUERY_STMT = "DELETE FROM countries WHERE \"countryCode\" = ?";

    private List<String> ids = new ArrayList<>(250);
    private List<String> countryCodes = new ArrayList<>(250);

    @Override
    public boolean importData() {
        return true;
    }

    public void prepare() throws Exception {
        if (ids.isEmpty() || countryCodes.isEmpty()) {
            doImportData();
            // setupOnce non-static
            SQLResponse response = execute("SELECT \"_id\", \"countryCode\" FROM countries");
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

    public DeleteByQueryRequest getDeleteApiByQueryRequest() throws Exception {

        return new DeleteByQueryRequest(INDEX_NAME).source(
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
            DeleteResponse response = client().execute(DeleteAction.INSTANCE, getDeleteApiByIdRequest()).actionGet();
            Assert.assertTrue(response.isFound());
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteApiByQuery() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            client().execute(DeleteByQueryAction.INSTANCE, getDeleteApiByQueryRequest()).actionGet();
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteSqlById() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            execute(DELETE_BY_ID_STMT, new Object[]{getDeleteId()});
        }
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testDeleteSQLByQuery() throws Exception {
        for (int i=0; i<NUM_REQUESTS_PER_TEST; i++) {
            execute(DELETE_BY_QUERY_STMT, new Object[]{ getCountryCode() });
        }
    }
}
