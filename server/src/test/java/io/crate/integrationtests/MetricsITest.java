/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetricsITest extends IntegTestCase {

    @Before
    public void clearStats() {
        execute("SET GLOBAL \"stats.enabled\" = FALSE");
        execute("SET GLOBAL \"stats.enabled\" = TRUE");
    }

    @After
    public void resetStats() {
        execute("RESET GLOBAL \"stats.enabled\"");
    }

    @Test
    public void testSimpleQueryOnMetrics() {
        execute("SELECT 1");

        execute("SELECT node, " +
                "node['id'], " +
                "node['name'], " +
                "min, " +
                "max, " +
                "mean, " +
                "percentiles, " +
                "percentiles['25'], " +
                "percentiles['50'], " +
                "percentiles['75'], " +
                "percentiles['90'], " +
                "percentiles['95'], " +
                "percentiles['99'], " +
                "total_count, " +
                "classification " +
                "FROM sys.jobs_metrics " +
                "WHERE classification['type'] = 'SELECT' " +
                "ORDER BY max DESC");

        for (Object[] row : response.rows()) {
            assertThat((long) row[3]).isGreaterThanOrEqualTo(0L);
            assertThat((long) row[4]).isGreaterThanOrEqualTo(0L);
            assertThat((double) row[5]).isGreaterThanOrEqualTo(0.0d);
            assertThat(row[13]).isEqualTo(1L);
        }
    }

    @Test
    public void testTotalCountOnMetrics() throws Exception {
        int numQueries = 100;
        for (int i = 0; i < numQueries; i++) {
            execute("SELECT 1");
        }

        assertBusy(() -> {
            execute("SELECT sum(total_count) FROM sys.jobs_metrics WHERE classification['type'] = 'SELECT'");
            assertThat(response.rows()[0][0]).isEqualTo((long) numQueries);
        });
    }

    @Test
    public void test_total_affected_row_count() throws Exception {
        execute("create table t (a int)");
        long x = randomInt(10) + 1;

        for (int i = 0; i < x; i++) {
            execute("insert into t values (1), (2), (3)");
        }
        execute("refresh table t");
        execute("update t set a=10 where a=1");
        execute("refresh table t");

        assertBusy(() -> {
            execute("SELECT sum(total_affected_row_count) FROM sys.jobs_metrics WHERE classification['type'] = 'INSERT'");
            assertThat(response.rows()[0][0]).isEqualTo(3 * x);
        });
        assertBusy(() -> {
            execute("SELECT sum(total_affected_row_count) FROM sys.jobs_metrics WHERE classification['type'] = 'UPDATE'");
            assertThat(response.rows()[0][0]).isEqualTo(x);
        });
    }

    @Test
    public void test_total_affected_row_count_from_bulk_operations() throws Exception {
        execute("create table t (a int primary key, b int)");
        execute("insert into t(a) values (?)", new Object[][]{{1}, {2}, {2}, {3}}); // 3 rows inserted
        execute("refresh table t");
        execute("update t set b=? where a>=?", new Object[][]{{1, 1}, {2, 2}}); // 5 rows (3+2) updated
        execute("refresh table t");

        assertBusy(() -> {
            execute("SELECT sum(total_affected_row_count) FROM sys.jobs_metrics WHERE classification['type'] = 'INSERT'");
            assertThat(response.rows()[0][0]).isEqualTo(3L);
        });
        assertBusy(() -> {
            execute("SELECT sum(total_affected_row_count) FROM sys.jobs_metrics WHERE classification['type'] = 'UPDATE'");
            assertThat(response.rows()[0][0]).isEqualTo(5L);
        });
    }
}
