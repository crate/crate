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

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.metadata.sys.MetricsView;
import io.crate.planner.Plan;

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

        // We record the data for the histograms **after** we notify the result receivers
        // (see {@link JobsLogsUpdateListener usage).
        // So it might happen that the recording of the statement the "SELECT 1" in the metrics is done
        // AFTER its execution has returned to this test (because async programming is evil like that).
        assertBusy(() -> {
            long cnt = 0;
            for (JobsLogService jobsLogService : cluster().getInstances(JobsLogService.class)) {
                for (MetricsView metrics: jobsLogService.get().metrics()) {
                    if (metrics.classification().type() == Plan.StatementType.SELECT) {
                        cnt += metrics.totalCount();
                    }
                }
            }
            assertThat(cnt).isEqualTo(numQueries);
        });

        execute("SELECT sum(total_count) FROM sys.jobs_metrics WHERE classification['type'] = 'SELECT'");
        assertThat(response.rows()[0][0]).isEqualTo((long) numQueries);
    }
}
