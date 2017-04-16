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
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.google.common.base.Joiner;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLRequest;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Collections;

@AxisRange(min = 0)
@BenchmarkMethodChart(filePrefix = "benchmark-bulk-insert")
public class BulkInsertBenchmark extends BenchmarkBase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TestRule benchmarkRun = RuleChain.outerRule(new BenchmarkRule()).around(super.ruleChain);

    public static final int BENCHMARK_ROUNDS = 10;
    public static final int ROWS = 10000;

    public static final String SINGLE_INSERT_SQL_STMT = "INSERT INTO countries " +
            "(\"countryName\", \"countryCode\", \"isoNumeric\", \"east\", \"north\", \"west\", \"south\"," +
            "\"isoAlpha3\", \"currencyCode\", \"continent\", \"continentName\", \"languages\", \"fipsCode\", \"capital\", \"population\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    public static String BULK_INSERT_SQL_STMT = "INSERT INTO countries " +
            "(\"countryName\", \"countryCode\", \"isoNumeric\", \"east\", \"north\", \"west\", \"south\"," +
            "\"isoAlpha3\", \"currencyCode\", \"continent\", \"continentName\", \"languages\", \"fipsCode\", \"capital\", \"population\") " +
            "VALUES " + Joiner.on(",").join(Collections.nCopies(ROWS, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

    private SQLRequest getBulkRequest() {
        Object[] bulkObjects = new Object[ROWS * 15];
        for(int i=0; i < ROWS; i++){
            Object[] row = getRandomObject();
            for(int j=0; j<15; j++){
                bulkObjects[i*15+j] = row[j];
            }
        }
        return new SQLRequest(BULK_INSERT_SQL_STMT, bulkObjects);
    }

    private Object[] getRandomObject() {
        return new Object[]{
                RandomStringUtils.randomAlphabetic(10), // countryName
                RandomStringUtils.randomAlphabetic(2),  // countryCode
                RandomStringUtils.randomAlphabetic(3),  // isoNumeric
                Math.random(),                          // east
                Math.random(),                          // north
                Math.random(),                          // west
                Math.random(),                          // south
                RandomStringUtils.randomAlphabetic(3),  // isoAlpha3
                RandomStringUtils.randomAlphabetic(3),  // currencyCode
                RandomStringUtils.randomAlphabetic(2),  // contintent
                RandomStringUtils.randomAlphabetic(10), // contintentName
                RandomStringUtils.randomAlphabetic(3),  // languages
                RandomStringUtils.randomAlphabetic(3),  // fipsCode
                RandomStringUtils.randomAlphabetic(10), // capital
                (int)Math.random(),                     // population
        };
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testBulkInsert() {
        getClient(false).execute(SQLAction.INSTANCE, getBulkRequest()).actionGet();
    }

    @BenchmarkOptions(benchmarkRounds = BENCHMARK_ROUNDS, warmupRounds = 1)
    @Test
    public void testMultipleIndexRequests() {
        for(int i=0; i < ROWS; i++){
            getClient(false).execute(SQLAction.INSTANCE,
                                    new SQLRequest(SINGLE_INSERT_SQL_STMT, getRandomObject())).actionGet();
        }
    }

}
