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

package io.crate.integrationtests;

import io.crate.action.sql.SQLResponse;
import io.crate.jobs.JobContextService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@TestLogging("io.crate.jobs.JobExecutionContext:TRACE")
@ElasticsearchIntegrationTest.ClusterScope (numDataNodes = 2)
public class LongRunningQueriesIntegrationTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final long ORIGINAL_KEEP_ALIVE = JobContextService.KEEP_ALIVE;
    private static final long TEST_KEEP_ALIVE = TimeValue.timeValueSeconds(2).millis();
    private static final int ROWS = 4;
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(20);


    @Override
    public SQLResponse execute(String stmt) {
        return super.execute(stmt, TIMEOUT);
    }

    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        return super.execute(stmt, args, TIMEOUT);
    }

    @Before
    public void prepare() {
        JobContextService.KEEP_ALIVE = TEST_KEEP_ALIVE;
    }

    @After
    public void cleanup() {
        JobContextService.KEEP_ALIVE = ORIGINAL_KEEP_ALIVE;
    }

    @Test
    public void testLongQueryThenFetchContextIsNotReapedWhileInCollector() throws Exception {
        execute("create table longt (id int, duration long) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        Object[][] args = new Object[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            args[i] = new Object[]{i, 1000};
        }
        execute("insert into longt (id, duration) values (?, ?)", args);
        execute("refresh table longt");

        // sleep for 4 seconds, 2 seconds more than KEEP_ALIVE
        // where clause is evaluated in LuceneDocCollector
        execute("select duration from longt where sleep(duration) = true");
    }

    @Test
    public void testLongQueryThenFetchContextIsNotReapedWhileInCollectorWithoutMatches() throws Exception {
        execute("create table longt (id int, duration long) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        Object[][] args = new Object[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            args[i] = new Object[]{i, 1000};
        }
        execute("insert into longt (id, duration) values (?, ?)", args);
        execute("refresh table longt");

        // sleep for 4 seconds, 3 seconds more than KEEP_ALIVE
        // where clause is evaluated in LuceneDocCollector
        execute("select duration from longt where sleep(duration) = false");
    }

    @TestLogging("io.crate.jobs.JobExecutionContext:TRACE")
    @Test
    public void testLongQueryThenFetchContextIsNotReapedWhileInFetchProjector() throws Exception {
        execute("create table longt (id int, duration long) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        Object[][] args = new Object[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            args[i] = new Object[]{i, 1000};
        }
        execute("insert into longt (id, duration) values (?, ?)", args);
        execute("refresh table longt");

        // sleep for 4 seconds, 3 seconds more than KEEP_ALIVE
        // duration will be fetched in FetchProjector and sleep will be executed
        // there (localMerge)
        execute("select sleep(duration) from longt");
    }
}
