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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class AddingTableColumnsBenchmark extends BenchmarkBase {

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static boolean tablesCreated = false;

    @Before
    public void setUp() throws Exception {
        if (!tablesCreated) {
            if (NODE1 == null) {
                NODE1 = cluster.startNode(getNodeSettings(1));
            }
            if (NODE2 == null) {
                NODE2 = cluster.startNode(getNodeSettings(2));
            }

            createTable(10, "table_10");
            createTable(5000, "table_5k");

            tablesCreated = true;
        }
    }

    private void createTable(int columnsAmount, String tableName) {
        StringBuilder randomColumns = new StringBuilder();
        for (int i = 0; i < columnsAmount; ++i) {
            randomColumns.append("col")
                    .append(getRandom().nextInt(Integer.MAX_VALUE))
                    .append(getRandom().nextInt(Integer.MAX_VALUE))
                    .append(" string,")
                    .append("int")
                    .append(getRandom().nextInt(Integer.MAX_VALUE))
                    .append(getRandom().nextInt(Integer.MAX_VALUE))
                    .append(" integer,");
        }

        execute("create table " + tableName + " (" +
                " countryName string," +
                randomColumns.toString() +
                " population integer," +
                " continent string" +
                ") clustered into 1 shards with (number_of_replicas=0)", new Object[0], false);

    }

    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 5)
    @Test
    public void testAlterTable10() throws Exception {
        for (int i = 0; i < 10; ++i) {
            execute("alter table table_10 add column " + getRandom().nextInt() + " string", new Object[0], true);
        }
    }

    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 5)
    @Test
    public void testAlterTable5k() throws Exception {
        for (int i = 0; i < 10; ++i) {
            execute("alter table table_5k add column " + getRandom().nextInt() + " string", new Object[0], true);
        }
    }

}
