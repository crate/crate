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

package io.crate.integrationtests;

import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedSchema;
import io.crate.testing.UseSemiJoins;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@UseRandomizedSchema(random = false) // Avoid set session stmt to interfere with tests
@UseSemiJoins(0) // Avoid set session stmt to interfere with tests
@UseHashJoins(0) // Avoid set session stmt to interfere with tests
public class JobLogIntegrationTest extends SQLTransportIntegrationTest {

    @After
    public void resetSettings() {
        // reset stats settings in case of some tests changed it and failed without resetting.
        execute("reset global stats.enabled, stats.jobs_log_size, stats.operations_log_size");
    }

    @Test
    @UseJdbc(0) // SET extra_float_digits = 3 gets added to the jobs_log
    public void testJobLogWithEnabledAndDisabledStats() throws Exception {
        execute("select name from sys.cluster");
        execute("select * from sys.jobs_log");
        assertThat(response.rowCount(), greaterThan(0L));

        // clear the logs; the next statements could all hit the same node
        // which would mean that "select name from sys.cluster" or "select * from sys.jobs_log" would still be present in sys.jobs
        // (Each node has its own local <jobs_log_size> entries)
        execute("set global transient stats.jobs_log_size=0");
        execute("set global transient stats.jobs_log_size=1");

        execute("select id from sys.cluster");
        execute("select id from sys.cluster");
        execute("select id from sys.cluster");
        execute("select stmt from sys.jobs_log order by ended desc");

        // there are 2 nodes so depending on whether both nodes were hit this should be either 1 or 2
        // but never 3 because the queue size is only 1
        assertThat(response.rowCount(), Matchers.lessThanOrEqualTo(2L));
        assertThat(response.rows()[0][0], is("select id from sys.cluster"));

        execute("set global transient stats.enabled = false");
        waitNoPendingTasksOnAll();
        execute("select * from sys.jobs_log");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    @UseJdbc(0) // set has no rowcount
    public void testSetSingleStatement() throws Exception {
        execute("select settings['stats']['jobs_log_size'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getDefault()));

        execute("set global persistent stats.enabled= true, stats.jobs_log_size=7");
        assertThat(response.rowCount(), is(1L));

        execute("select settings['stats']['jobs_log_size'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(7));

        execute("reset global stats.jobs_log_size");
        assertThat(response.rowCount(), is(1L));
        waitNoPendingTasksOnAll();

        execute("select settings['stats']['enabled'], settings['stats']['jobs_log_size'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is(JobsLogService.STATS_ENABLED_SETTING.getDefault()));
        assertThat(response.rows()[0][1], is(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getDefault()));

    }

    @Test
    public void testEmptyJobsInLog() throws Exception {
        // Setup data
        execute("create table characters (id int primary key, name string)");
        sqlExecutor.ensureYellowOrGreen();

        execute("set global transient stats.enabled = true");
        execute("insert into characters (id, name) values (1, 'sysjobstest')");
        execute("refresh table characters");
        execute("delete from characters where id = 1");
        // make sure everything is deleted (nothing changed in whole class lifecycle cluster state)
        assertThat(response.rowCount(), is(1L));
        execute("refresh table characters");

        execute("select * from sys.jobs_log where stmt like 'insert into%' or stmt like 'delete%'");
        assertThat(response.rowCount(), is(2L));
    }
}
