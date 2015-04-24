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

package io.crate.module.sql.benchmark;


import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import io.crate.TimestampFormat;
import io.crate.action.sql.SQLBulkAction;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.junit.AfterClass;
import org.junit.Before;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@AxisRange(min = 0)
@BenchmarkHistoryChart(filePrefix="benchmark-partitioned-bulk-insert-history")
@BenchmarkMethodChart(filePrefix = "benchmark-partitioned-bulk-insert")
public class PartitionedBulkInsertBenchmark extends BenchmarkBase {

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final String INDEX_NAME = "motiondata";
    public static final int BENCHMARK_ROUNDS = 3;
    public static final int ROWS = 5000;
    public static final int UNIQUE_PARTITIONS = 200;

    private static final String[] partitions = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");
    private int partitionIndex = 0;
    private static final long TS = TimestampFormat.parseTimestampString("2015-01-01");
    public static final String SINGLE_INSERT_SQL_STMT = "insert into motiondata (d, device_id, ts, ax) values (?,?,?,?)";


    @Before
    public void setUp() throws Exception {
        if (NODE1 == null) {
            NODE1 = cluster.startNode(getNodeSettings(1));
        }
        if (NODE2 == null) {
            NODE2 = cluster.startNode(getNodeSettings(2));
        }
        if(!indexExists()){
            execute("create table motiondata (\n" +
                    "  d string,\n" +
                    "  device_id string,\n" +
                    "  ts timestamp,\n" +
                    "  ax double,\n" +
                    "  primary key (d, device_id, ts)\n" +
                    ")\n" +
                    "partitioned by (d)\n" +
                    "clustered by (device_id)", new Object[0], false);
            refresh(client());
        }

    }

    @AfterClass
    public static void afterClass() {
        cluster.client().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
    }

    @Override
    public boolean indexExists() {
        return getClient(false).admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().isExists();
    }

    private SQLBulkRequest getBulkArgsRequest(boolean uniquePartitions, int numRows) {
        Object[][] bulkArgs = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            bulkArgs[i] = getRandomObject(uniquePartitions);
        }
        return new SQLBulkRequest(SINGLE_INSERT_SQL_STMT, bulkArgs);
    }

    private Object[] getRandomObject(boolean uniquePartitions) {
        int partitionIdx = partitionIndex++;
        String partitionValue = uniquePartitions ? String.valueOf(partitionIdx) : partitions[(partitionIdx) % partitions.length];
        return new Object[]{
                    partitionValue,
                    RandomStringUtils.randomAlphabetic(1),
                    TS + partitionIdx,
                    5.0};
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    public void testBulkInsertWithBulkArgs() throws Exception {
        long inserted = 0;
        long errors = 0;

        SQLBulkResponse bulkResponse = getClient(false).execute(SQLBulkAction.INSTANCE, getBulkArgsRequest(false, ROWS)).actionGet();
        for (SQLBulkResponse.Result result : bulkResponse.results()) {
            assertThat(result.errorMessage(), is(nullValue()));
            if (result.rowCount() < 0) {
                errors++;
            } else {
                inserted += result.rowCount();
            }
        }
        assertThat(errors, is(0L));
        assertThat(inserted, is(5000L));
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 1)
    public void testBulkInsertWithUniquePartitions() throws Exception {
        long inserted = 0;
        long errors = 0;

        SQLBulkResponse bulkResponse = getClient(false)
                .execute(SQLBulkAction.INSTANCE, getBulkArgsRequest(true, UNIQUE_PARTITIONS))
                .actionGet();
        for (SQLBulkResponse.Result result : bulkResponse.results()) {
            assertThat(result.errorMessage(), is(nullValue()));
            if (result.rowCount() < 0) {
                errors++;
            } else {
                inserted += result.rowCount();
            }
        }
        assertThat(errors, is(0L));
        assertThat(inserted, is((long)UNIQUE_PARTITIONS));

    }
}
